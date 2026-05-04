import { createServer } from "node:http";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { existsSync } from "node:fs";
import path from "node:path";
import { createUmi } from "@metaplex-foundation/umi-bundle-defaults";
import { keypairIdentity, publicKey as umiPublicKey } from "@metaplex-foundation/umi";
import {
  fetchMetadataFromSeeds,
  findMasterEditionPda,
  findMetadataPda,
  mplTokenMetadata,
  verifyCollectionV1,
} from "@metaplex-foundation/mpl-token-metadata";
import { clusterApiUrl, Connection, PublicKey } from "@solana/web3.js";

const rootDir = process.cwd();
const dataDir = path.join(rootDir, "data");
const registryPath = path.join(dataDir, "registry.json");
const ppegMintAddress = "pfKAC56v3mb661Kwd2ZK9sMWrGMbS2UHm5tj124ppeg";
const ppegMint = new PublicKey(ppegMintAddress);
const tokenProgram = new PublicKey("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
const maxSupply = 2500;
const healthcheckPort = Number(process.env.PORT || 8080);
const publicPort = Number(process.env.REGISTRY_PORT || 8787);
const listenPorts = [...new Set([healthcheckPort, publicPort].filter(Boolean))];
const reindexIntervalMs = Number(process.env.REINDEX_INTERVAL_MS || 3000);
const solanaRpcUrl = process.env.SOLANA_RPC_URL || process.env.VITE_SOLANA_RPC_URL || clusterApiUrl("mainnet-beta");
const collectionMintAddress = process.env.COLLECTION_MINT || process.env.VITE_COLLECTION_MINT || "";
const collectionAuthorityKeypairPath = process.env.COLLECTION_AUTHORITY_KEYPAIR || process.env.DEV_KEYPAIR || "";
const convertProgramAddress = process.env.CONVERT_PROGRAM_ID || process.env.VITE_CONVERT_PROGRAM_ID || "";
const registryProgramAddress = process.env.REGISTRY_PROGRAM_ID || process.env.VITE_REGISTRY_PROGRAM_ID || "AF5wG7FArPd4GdUMNGj6hevhKT8GCHRBuHbMpbDnYz2K";
const verifyAssetBaseUrl = String(process.env.VERIFY_ASSET_BASE_URL || process.env.VITE_ASSET_BASE_URL || "").replace(/\/$/, "");
const solanaRpcUrls = [
  solanaRpcUrl,
  "https://api.mainnet-beta.solana.com",
  "https://solana-rpc.publicnode.com",
].filter((url, index, urls) => url && urls.indexOf(url) === index);
const excludedRegistryWallets = new Set(
  String(process.env.EXCLUDED_REGISTRY_WALLETS || process.env.PPEG_POOL_WALLETS || "")
    .split(/[\s,]+/)
    .map(normalizeWallet)
    .filter(Boolean),
);
let registryWriteQueue = Promise.resolve();
let collectionVerifier = null;
let reindexPromise = null;
const registryProgram = new PublicKey(registryProgramAddress);
const holderCountOffset = 10;
const holderOwnerOffset = 16;
const holderMintOffset = 48;
const holderIdsOffset = 80;

function hashAddress(address) {
  let hash = 2166136261;
  for (let index = 0; index < address.length; index += 1) {
    hash ^= address.charCodeAt(index);
    hash = Math.imul(hash, 16777619);
  }
  return hash >>> 0;
}

async function loadRegistry() {
  if (!existsSync(registryPath)) {
    return { assignments: {}, ids: {} };
  }

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

function normalizePegIds(ids) {
  return [...new Set((Array.isArray(ids) ? ids : [])
    .map(Number)
    .filter((id) => Number.isInteger(id) && id >= 1 && id <= maxSupply))];
}

function removeIdFromAssignment(registry, wallet, id) {
  const assignment = registry.assignments[wallet];
  if (!assignment) return;
  assignment.ids = normalizePegIds(assignment.ids).filter((item) => item !== id);
  assignment.status = assignment.ids.length > 0 ? "active" : "inactive";
}

function sanitizeRegistry(registry) {
  registry.assignments = registry.assignments && typeof registry.assignments === "object" ? registry.assignments : {};
  registry.ids = registry.ids && typeof registry.ids === "object" ? registry.ids : {};

  const cleanIds = {};
  for (const [rawId, wallet] of Object.entries(registry.ids)) {
    const id = Number(rawId);
    const owner = normalizeWallet(wallet);
    if (!Number.isInteger(id) || id < 1 || id > maxSupply || !owner || isExcludedRegistryWallet(owner)) continue;
    if (!cleanIds[String(id)]) cleanIds[String(id)] = owner;
  }
  registry.ids = cleanIds;

  for (const [wallet, assignment] of Object.entries(registry.assignments)) {
    const owner = normalizeWallet(assignment?.wallet || wallet);
    if (!owner || isExcludedRegistryWallet(owner)) {
      for (const id of normalizePegIds(assignment?.ids)) delete registry.ids[String(id)];
      delete registry.assignments[wallet];
      continue;
    }
    const ids = normalizePegIds(assignment?.ids).filter((id) => registry.ids[String(id)] === owner);
    registry.assignments[owner] = {
      ...assignment,
      wallet: owner,
      ids,
      balance: Number(assignment?.balance || 0),
      status: ids.length > 0 ? "active" : "inactive",
    };
    if (owner !== wallet) delete registry.assignments[wallet];
  }

  return registry;
}

async function withRegistryWrite(mutator) {
  const run = registryWriteQueue.then(async () => {
    const registry = sanitizeRegistry(await loadRegistry());
    const result = await mutator(registry);
    sanitizeRegistry(registry);
    await saveRegistry(registry);
    return result ?? registry;
  });
  registryWriteQueue = run.catch(() => {});
  return run;
}

function normalizeWallet(wallet) {
  return String(wallet || "").trim();
}

function cleanMetadataText(value) {
  return String(value || "").replace(/\0/g, "").trim();
}

function padId(id) {
  return String(Number(id)).padStart(4, "0");
}

function collectionOption(metadata) {
  if (!metadata?.collection) return null;
  if (metadata.collection.__option === "Some") return metadata.collection.value;
  if (metadata.collection.key) return metadata.collection;
  return null;
}

async function getCollectionVerifier() {
  if (collectionVerifier) return collectionVerifier;
  if (!collectionMintAddress) throw new Error("COLLECTION_MINT is not configured");
  if (!collectionAuthorityKeypairPath) throw new Error("COLLECTION_AUTHORITY_KEYPAIR is not configured");

  const secretText = collectionAuthorityKeypairPath.trim().startsWith("[")
    ? collectionAuthorityKeypairPath
    : await readFile(collectionAuthorityKeypairPath, "utf8");
  const secret = JSON.parse(secretText);
  const umi = createUmi(solanaRpcUrl).use(mplTokenMetadata());
  const keypair = umi.eddsa.createKeypairFromSecretKey(new Uint8Array(secret));
  umi.use(keypairIdentity(keypair));

  const collectionMint = umiPublicKey(collectionMintAddress);
  collectionVerifier = {
    umi,
    collectionMint,
    collectionMetadata: findMetadataPda(umi, { mint: collectionMint }),
    collectionMasterEdition: findMasterEditionPda(umi, { mint: collectionMint }),
  };
  return collectionVerifier;
}

async function verifyCollectionNft(nftMintText, pegId) {
  if (!nftMintText) throw new Error("nftMint is required");
  await assertConvertedNft({ nftMintText, pegId });
  const verifier = await getCollectionVerifier();
  const mint = umiPublicKey(nftMintText);
  const metadata = findMetadataPda(verifier.umi, { mint });
  const before = await fetchMetadataFromSeeds(verifier.umi, { mint });
  const collection = collectionOption(before);

  if (!collection || String(collection.key) !== String(verifier.collectionMint)) {
    throw new Error(`NFT metadata collection is not ${verifier.collectionMint}`);
  }

  const name = cleanMetadataText(before.name);
  const symbol = cleanMetadataText(before.symbol);
  const uri = cleanMetadataText(before.uri);
  if (name !== `PEPE PEG #${padId(pegId)}`) throw new Error("NFT name does not match pegId");
  if (symbol !== "PPEG") throw new Error("NFT symbol is not PPEG");
  if (verifyAssetBaseUrl && uri !== `${verifyAssetBaseUrl}/json/${Number(pegId)}.json`) {
    throw new Error("NFT metadata URI is not from the configured asset base");
  }

  if (collection.verified) {
    return {
      event: "already_verified",
      nftMint: String(mint),
      collectionMint: String(verifier.collectionMint),
      verified: true,
    };
  }

  const signature = await verifyCollectionV1(verifier.umi, {
    authority: verifier.umi.identity,
    metadata,
    collectionMint: verifier.collectionMint,
    collectionMetadata: verifier.collectionMetadata,
    collectionMasterEdition: verifier.collectionMasterEdition,
  }).sendAndConfirm(verifier.umi);
  const after = await fetchMetadataFromSeeds(verifier.umi, { mint });
  const verifiedCollection = collectionOption(after);

  return {
    event: "verified_collection",
    nftMint: String(mint),
    collectionMint: String(verifier.collectionMint),
    verified: Boolean(verifiedCollection?.verified),
    signature: Buffer.from(signature.signature).toString("base64"),
  };
}

async function assertConvertedNft({ nftMintText, pegId }) {
  if (!convertProgramAddress) throw new Error("CONVERT_PROGRAM_ID is not configured");
  const id = Number(pegId);
  if (!Number.isInteger(id) || id < 1 || id > maxSupply) throw new Error("valid pegId is required");

  const convertProgram = new PublicKey(convertProgramAddress);
  const nftMint = new PublicKey(nftMintText);
  const [conversionAccount] = PublicKey.findProgramAddressSync(
    [Buffer.from("conversion"), ppegMint.toBuffer(), Buffer.from([id & 0xff, id >> 8])],
    convertProgram,
  );
  const account = await new Connection(solanaRpcUrl, "confirmed").getAccountInfo(conversionAccount);
  if (!account || account.owner.toBase58() !== convertProgram.toBase58()) {
    throw new Error("conversion account not found");
  }
  const data = account.data;
  if (data.subarray(0, 8).toString("utf8") !== "PPEGCVT1") throw new Error("invalid conversion account");
  if (data[8] !== 1) throw new Error("conversion is not active");
  if (data.readUInt16LE(12) !== id) throw new Error("conversion pegId mismatch");
  if (new PublicKey(data.subarray(48, 80)).toBase58() !== ppegMint.toBase58()) {
    throw new Error("conversion pPEG mint mismatch");
  }
  if (new PublicKey(data.subarray(80, 112)).toBase58() !== nftMint.toBase58()) {
    throw new Error("conversion NFT mint mismatch");
  }
}

function isExcludedRegistryWallet(wallet) {
  return excludedRegistryWallets.has(normalizeWallet(wallet));
}

function claimableCount(balance) {
  return Math.max(0, Math.min(maxSupply, Math.floor(Number(balance || 0))));
}

async function fetchPpegBalance(wallet) {
  if (isExcludedRegistryWallet(wallet)) return 0;

  const owner = new PublicKey(wallet);
  let lastError = null;

  for (const rpcUrl of solanaRpcUrls) {
    try {
      const connection = new Connection(rpcUrl, "confirmed");
      const accounts = await connection.getParsedTokenAccountsByOwner(owner, { mint: ppegMint });

      return accounts.value.reduce((total, account) => {
        const tokenAmount = account.account.data.parsed.info.tokenAmount;
        return total + Number(tokenAmount.uiAmountString || tokenAmount.uiAmount || 0);
      }, 0);
    } catch (error) {
      lastError = error;
      console.warn(`pPEG balance RPC failed: ${rpcUrl}`, error);
    }
  }

  throw lastError || new Error("All Solana RPC endpoints failed");
}

function refreshRegistryOnce() {
  if (!reindexPromise) {
    reindexPromise = refreshRegistryFromOnchain()
      .catch(async (error) => {
        console.warn("registry refresh failed", error);
        return sanitizeRegistry(await loadRegistry());
      })
      .finally(() => {
        reindexPromise = null;
      });
  }
  return reindexPromise;
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

function syncAssignment(wallet, balance, registry, onchainIds = []) {
  if (isExcludedRegistryWallet(wallet)) {
    const existing = registry.assignments[wallet] || { ids: [] };
    for (const id of normalizePegIds(existing.ids)) delete registry.ids[String(id)];
    delete registry.assignments[wallet];
    return { wallet, ids: [], balance: 0, status: "excluded", updatedAt: new Date().toISOString() };
  }

  const targetCount = claimableCount(balance);
  const existing = registry.assignments[wallet] || { ids: [] };
  const ids = [];
  const preferredIds = normalizePegIds(onchainIds).slice(0, targetCount);

  for (const id of preferredIds) {
    const previousWallet = registry.ids[String(id)];
    if (previousWallet && previousWallet !== wallet) removeIdFromAssignment(registry, previousWallet, id);
    registry.ids[String(id)] = wallet;
    ids.push(id);
  }

  for (const id of normalizePegIds(existing.ids)) {
    if (registry.ids[String(id)] === wallet && !ids.includes(id) && ids.length < targetCount) {
      ids.push(id);
    }
  }
  for (const id of normalizePegIds(existing.ids)) {
    if (!ids.includes(id)) {
      delete registry.ids[String(id)];
    }
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
  let lastError = null;

  for (const rpcUrl of solanaRpcUrls) {
    try {
      const connection = new Connection(rpcUrl, "confirmed");
      const accounts = await connection.getParsedProgramAccounts(tokenProgram, {
        filters: [
          { dataSize: 165 },
          { memcmp: { offset: 0, bytes: ppegMintAddress } },
        ],
      });

      const balances = new Map();
      for (const account of accounts) {
        const info = account.account.data.parsed?.info;
        const wallet = info?.owner;
        const tokenAmount = info?.tokenAmount;
        const balance = Number(tokenAmount?.uiAmountString || tokenAmount?.uiAmount || 0);
        if (!wallet || isExcludedRegistryWallet(wallet) || balance <= 0) continue;
        balances.set(wallet, (balances.get(wallet) || 0) + balance);
      }

      return Array.from(balances, ([wallet, balance]) => ({ wallet, balance }))
        .sort((a, b) => a.wallet.localeCompare(b.wallet));
    } catch (error) {
      lastError = error;
      console.warn(`pPEG holder scan RPC failed: ${rpcUrl}`, error);
    }
  }

  throw lastError || new Error("All Solana RPC endpoints failed");
}

function registryHolderPda(wallet) {
  return PublicKey.findProgramAddressSync(
    [Buffer.from("holder"), ppegMint.toBuffer(), new PublicKey(wallet).toBuffer()],
    registryProgram,
  )[0];
}

function parseHolderIds(data, wallet) {
  if (!data || data.length < holderIdsOffset || data.subarray(0, 8).toString("utf8") !== "PPEGHLD1") return [];
  const owner = new PublicKey(data.subarray(holderOwnerOffset, holderOwnerOffset + 32)).toBase58();
  const mint = new PublicKey(data.subarray(holderMintOffset, holderMintOffset + 32)).toBase58();
  if (owner !== wallet || mint !== ppegMintAddress) return [];

  const count = data.readUInt16LE(holderCountOffset);
  const ids = [];
  for (let index = 0; index < count; index += 1) {
    const offset = holderIdsOffset + index * 2;
    if (offset + 2 > data.length) break;
    const id = data.readUInt16LE(offset);
    if (id >= 1 && id <= maxSupply) ids.push(id);
  }
  return normalizePegIds(ids);
}

async function fetchRegistryHolderIds(wallet) {
  if (!wallet || isExcludedRegistryWallet(wallet)) return [];
  const holder = registryHolderPda(wallet);
  let lastError = null;

  for (const rpcUrl of solanaRpcUrls) {
    try {
      const connection = new Connection(rpcUrl, "confirmed");
      const account = await connection.getAccountInfo(holder, "confirmed");
      return account ? parseHolderIds(account.data, wallet) : [];
    } catch (error) {
      lastError = error;
      console.warn(`registry holder RPC failed: ${rpcUrl}`, error);
    }
  }

  if (lastError) throw lastError;
  return [];
}

async function syncWalletFromOnchain(wallet) {
  const [balance, holderIds] = await Promise.all([
    fetchPpegBalance(wallet),
    fetchRegistryHolderIds(wallet),
  ]);
  return withRegistryWrite((registry) => syncAssignment(wallet, balance, registry, holderIds));
}

async function refreshRegistryFromOnchain() {
  const holders = await fetchPpegHolders();
  const holderIdsByWallet = new Map(await Promise.all(
    holders.map(async (holder) => [holder.wallet, await fetchRegistryHolderIds(holder.wallet).catch(() => [])]),
  ));

  return withRegistryWrite((registry) => {
    const activeWallets = new Set();

    for (const holder of holders) {
      if (isExcludedRegistryWallet(holder.wallet) || claimableCount(holder.balance) < 1) continue;
      activeWallets.add(holder.wallet);
      syncAssignment(holder.wallet, holder.balance, registry, holderIdsByWallet.get(holder.wallet) || []);
    }

    for (const wallet of Object.keys(registry.assignments)) {
      if (!activeWallets.has(wallet)) {
        syncAssignment(wallet, 0, registry);
      }
    }

    registry.lastIndexedAt = new Date().toISOString();
    registry.holderCount = holders.filter((holder) => claimableCount(holder.balance) > 0).length;
    return registry;
  });
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
    "Cache-Control": "no-store, max-age=0",
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
      send(response, 200, { ok: true, service: "pepe-peg-registry" });
      return;
    }

    if (request.method === "GET" && url.pathname === "/registry") {
      const wallet = normalizeWallet(url.searchParams.get("wallet"));
      if (wallet) {
        const assignment = await syncWalletFromOnchain(wallet);
        send(response, 200, assignment || null);
        return;
      }
      const registry = sanitizeRegistry(await loadRegistry());
      send(response, 200, registry);
      return;
    }

    if ((request.method === "GET" || request.method === "POST") && url.pathname === "/registry/refresh") {
      const registry = await refreshRegistryOnce();
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

      const assignment = await syncWalletFromOnchain(wallet);
      send(response, 200, assignment);
      return;
    }

    if (request.method === "POST" && url.pathname === "/collection/verify") {
      const body = await readBody(request);
      const result = await verifyCollectionNft(body.nftMint || body.mint, body.pegId);
      send(response, 200, result);
      return;
    }

    send(response, 404, { error: "not found" });
  } catch (error) {
    send(response, 500, { error: error.message || "server error" });
  }
}

for (const port of listenPorts) {
  createServer(handleRequest).listen(port, "0.0.0.0", () => {
    console.log(`PEPE PEG registry API listening on http://0.0.0.0:${port}`);
  });
}

setTimeout(refreshRegistryOnce, 3000);
setInterval(refreshRegistryOnce, reindexIntervalMs);
