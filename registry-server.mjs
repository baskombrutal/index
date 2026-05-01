import { createServer } from "node:http";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import path from "node:path";
import { clusterApiUrl, Connection, PublicKey } from "@solana/web3.js";

const dataDir = path.join(process.cwd(), "data");
const registryPath = path.join(dataDir, "registry.json");
const ppegMintAddress = "pfKAC56v3mb661Kwd2ZK9sMWrGMbS2UHm5tj124ppeg";
const ppegMint = new PublicKey(ppegMintAddress);
const tokenProgram = new PublicKey("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
const maxSupply = 2500;
const healthcheckPort = Number(process.env.PORT || 8080);
const publicPort = Number(process.env.REGISTRY_PORT || 8787);
const listenPorts = [...new Set([healthcheckPort, publicPort].filter(Boolean))];
const solanaRpcUrl = process.env.SOLANA_RPC_URL || clusterApiUrl("mainnet-beta");
const solanaRpcUrls = [
  solanaRpcUrl,
  "https://api.mainnet-beta.solana.com",
  "https://solana-rpc.publicnode.com",
].filter((url, index, urls) => url && urls.indexOf(url) === index);

function hashAddress(address) {
  let hash = 2166136261;
  for (let index = 0; index < address.length; index += 1) {
    hash ^= address.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

async function loadRegistry() {
  if (!existsSync(registryPath)) return { assignments: {}, ids: {} };

  try {
    return JSON.parse(await readFile(registryPath, "utf8"));
  } catch {
    return { assignments: {}, ids: {} };
  }
}

async function saveRegistry(registry) {
  await mkdir(dataDir, { recursive: true });
  await writeFile(registryPath, `${JSON.stringify(registry, null, 2)}\n`);
}

function normalizeWallet(wallet) {
  return String(wallet || "").trim();
}

function claimableCount(balance) {
  return Math.max(0, Math.min(maxSupply, Math.floor(Number(balance || 0))));
}

async function withRpc(callback) {
  let lastError = null;

  for (const rpcUrl of solanaRpcUrls) {
    try {
      const connection = new Connection(rpcUrl, "confirmed");
      return await callback(connection, rpcUrl);
    } catch (error) {
      lastError = error;
      console.warn(`Solana RPC failed: ${rpcUrl}`, error?.message || error);
    }
  }

  throw lastError || new Error("All Solana RPC endpoints failed");
}

async function fetchPpegBalance(wallet) {
  const owner = new PublicKey(wallet);
  const accounts = await withRpc((connection) => connection.getParsedTokenAccountsByOwner(owner, { mint: ppegMint }));

  return accounts.value.reduce((total, account) => {
    const tokenAmount = account.account.data.parsed.info.tokenAmount;
    return total + Number(tokenAmount.uiAmountString || tokenAmount.uiAmount || 0);
  }, 0);
}

function nextAvailableId(wallet, registry, reservedIds = []) {
  const start = (hashAddress(wallet) % maxSupply) + 1;
  const reserved = new Set(reservedIds.map(String));

  for (let offset = 0; offset < maxSupply; offset += 1) {
    const id = ((start + offset - 1) % maxSupply) + 1;
    if (reserved.has(String(id))) continue;
    const assignedWallet = registry.ids[String(id)];
    if (!assignedWallet || assignedWallet === wallet) return id;
  }

  return null;
}

function syncAssignment(wallet, balance, registry) {
  const targetCount = claimableCount(balance);
  const existing = registry.assignments[wallet] || { ids: [] };
  const ids = existing.ids.filter((id) => registry.ids[String(id)] === wallet);

  for (const id of existing.ids) {
    if (!ids.includes(id)) delete registry.ids[String(id)];
  }

  while (ids.length > targetCount) {
    const removed = ids.pop();
    delete registry.ids[String(removed)];
  }

  while (ids.length < targetCount) {
    const id = nextAvailableId(wallet, registry, ids);
    if (!id) break;
    ids.push(id);
    registry.ids[String(id)] = wallet;
  }

  registry.assignments[wallet] = {
    wallet,
    ids,
    balance: Number(balance || 0),
    status: ids.length > 0 ? "active" : "inactive",
    updatedAt: new Date().toISOString(),
  };

  return registry.assignments[wallet];
}

async function fetchPpegHolders() {
  const accounts = await withRpc((connection) => connection.getParsedProgramAccounts(tokenProgram, {
    filters: [
      { dataSize: 165 },
      { memcmp: { offset: 0, bytes: ppegMintAddress } },
    ],
  }));

  const balances = new Map();
  for (const account of accounts) {
    const info = account.account.data.parsed?.info;
    const wallet = info?.owner;
    const tokenAmount = info?.tokenAmount;
    const balance = Number(tokenAmount?.uiAmountString || tokenAmount?.uiAmount || 0);
    if (!wallet || balance <= 0) continue;
    balances.set(wallet, (balances.get(wallet) || 0) + balance);
  }

  return Array.from(balances, ([wallet, balance]) => ({ wallet, balance }))
    .sort((a, b) => a.wallet.localeCompare(b.wallet));
}

async function refreshRegistryFromOnchain() {
  const holders = await fetchPpegHolders();
  const registry = await loadRegistry();
  const activeWallets = new Set();

  for (const holder of holders) {
    if (claimableCount(holder.balance) < 1) continue;
    activeWallets.add(holder.wallet);
    syncAssignment(holder.wallet, holder.balance, registry);
  }

  for (const wallet of Object.keys(registry.assignments)) {
    if (!activeWallets.has(wallet)) syncAssignment(wallet, 0, registry);
  }

  registry.lastIndexedAt = new Date().toISOString();
  registry.holderCount = holders.filter((holder) => claimableCount(holder.balance) > 0).length;
  await saveRegistry(registry);
  return registry;
}

async function readBody(request) {
  const chunks = [];
  for await (const chunk of request) chunks.push(chunk);
  const raw = Buffer.concat(chunks).toString("utf8");
  return raw ? JSON.parse(raw) : {};
}

function send(response, statusCode, body) {
  response.writeHead(statusCode, {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type",
    "Content-Type": "application/json",
  });
  response.end(JSON.stringify(body));
}

async function handleRequest(request, response) {
  if (request.method === "OPTIONS") {
    send(response, 204, {});
    return;
  }

  const url = new URL(request.url, `http://${request.headers.host}`);

  try {
    if (request.method === "GET" && url.pathname === "/") {
      send(response, 200, { ok: true, service: "pepe-peg-registry", ports: listenPorts });
      return;
    }

    if (request.method === "GET" && url.pathname === "/health") {
      send(response, 200, { ok: true, ports: listenPorts });
      return;
    }

    if (request.method === "GET" && url.pathname === "/registry") {
      const wallet = normalizeWallet(url.searchParams.get("wallet"));
      const registry = await loadRegistry();
      send(response, 200, wallet ? registry.assignments[wallet] || null : registry);
      return;
    }

    if ((request.method === "GET" || request.method === "POST") && url.pathname === "/registry/refresh") {
      const registry = await refreshRegistryFromOnchain();
      send(response, 200, registry);
      return;
    }

    if (request.method === "POST" && url.pathname === "/registry/sync") {
      const body = await readBody(request);
      const wallet = normalizeWallet(body.wallet);
      if (!wallet) {
        send(response, 400, { error: "wallet is required" });
        return;
      }

      const balance = await fetchPpegBalance(wallet);
      const registry = await loadRegistry();
      const assignment = syncAssignment(wallet, balance, registry);
      await saveRegistry(registry);
      send(response, 200, assignment);
      return;
    }

    send(response, 404, { error: "not found" });
  } catch (error) {
    console.error(error);
    send(response, 500, { error: error.message || "server error" });
  }
}

for (const port of listenPorts) {
  const server = createServer(handleRequest);

  server.on("error", (error) => {
    console.error(`Registry API failed to start on port ${port}`, error);
    process.exit(1);
  });

  server.listen(port, "0.0.0.0", () => {
    console.log(`PEPE PEG registry API listening on 0.0.0.0:${port}`);
  });
}
