import {
  getAssetIdsBySlug,
  getConditionIdBySlug,
  fetchTrades,
  TradeItem,
} from "./polymarket";
import { subscribeMarket, subscribeOrdersMatched } from "./clob-ws";
import { promises as fs } from "fs";
import path from "path";

/**
 * Parse slug to extract base pattern and session timestamp
 * Example: "btc-updown-15m-1765979100" -> { base: "btc-updown-15m", timestamp: 1765979100 }
 */
function parseSlug(slug: string): { base: string; timestamp: number } | null {
  const match = slug.match(/^(.+)-(\d+)$/);
  if (!match) return null;
  return {
    base: match[1],
    timestamp: parseInt(match[2], 10),
  };
}

/**
 * Generate next session slug (15 minutes later)
 */
function getNextSessionSlug(currentSlug: string): string | null {
  const parsed = parseSlug(currentSlug);
  if (!parsed) return null;

  // Add 15 minutes (900 seconds) to get next session
  const nextTimestamp = parsed.timestamp + 900;
  return `${parsed.base}-${nextTimestamp}`;
}

/**
 * Calculate milliseconds until next session starts
 */
function getMsUntilNextSession(slug: string): number {
  const parsed = parseSlug(slug);
  if (!parsed) return Infinity;

  const sessionStartTime = parsed.timestamp * 1000; // Convert to milliseconds
  const sessionEndTime = sessionStartTime + 15 * 60 * 1000; // Add 15 minutes
  const now = Date.now();
  const msUntilNext = sessionEndTime - now;

  return Math.max(0, msUntilNext);
}

/**
 * Get the current active session slug based on current time
 * Sessions start at :00, :15, :30, :45 of each hour
 */
function getCurrentSessionSlug(basePattern: string): string {
  const now = new Date();
  const minutes = now.getMinutes();

  // Round down to the nearest 15-minute mark
  const sessionMinutes = Math.floor(minutes / 15) * 15;

  // Create a date for the current session start
  const sessionStart = new Date(now);
  sessionStart.setMinutes(sessionMinutes, 0, 0);

  const timestamp = Math.floor(sessionStart.getTime() / 1000);
  return `${basePattern}-${timestamp}`;
}

/**
 * Check if a slug's session is still active or has expired
 */
function isSessionActive(slug: string): boolean {
  const parsed = parseSlug(slug);
  if (!parsed) return false;

  const sessionStartTime = parsed.timestamp * 1000;
  const sessionEndTime = sessionStartTime + 15 * 60 * 1000;
  const now = Date.now();

  return now >= sessionStartTime && now < sessionEndTime;
}

async function main() {
  // ---- Config ----
  // List of base patterns to track. Timestamp will be automatically set to current session
  // Example: ["btc-updown-15m", "eth-updown-15m"]
  const basePatterns = [
    "btc-updown-15m",
    "eth-updown-15m",
    "sol-updown-15m",
    // Add more base patterns here
  ];

  // Generate slugs with current session timestamp
  const initialSlugs = basePatterns.map((base) => getCurrentSessionSlug(base));

  console.log("📅 Current session slugs:");
  initialSlugs.forEach((slug, idx) => {
    console.log(`   ${idx + 1}. ${slug} (from base: ${basePatterns[idx]})`);
  });

  // Optional: Map of wallets to track with custom names. If empty/undefined, track all wallets
  // Format: { "wallet_address": "wallet_name" }
  // Example: { "0x123...": "Wallet 1", "0x456...": "Wallet 2" }
  // Or use array format: ["0x123...", "0x456..."] (no names)
  // const walletsToTrack: Map<string, string> | undefined = undefined;
  const walletsToTrack = new Map<string, string>([
    ["0x751a2b86cab503496efd325c8344e10159349ea1", "Wallet1"],
    ["0x818f214c7f3e479cce1d964d53fe3db7297558cb", "Wallet2"],
    ["0x1ff49fdcb6685c94059b65620f43a683be0ce7a5", "Wallet3"],
  ]);
  // Alternative: Simple array format (no names)
  // const walletsToTrack = [
  //   "0x6031b6eed1c97e853c6e0f03ad3ce3529351f96d",
  //   "0x9de851c79a04376a356f22f7085d94d71795d837",
  // ];

  // Process each slug and determine current active sessions
  interface SlugState {
    base: string;
    currentSlug: string;
    websocket: ReturnType<typeof subscribeOrdersMatched> | null;
  }

  const slugStates: SlugState[] = [];

  for (const initialSlug of initialSlugs) {
    const parsed = parseSlug(initialSlug);
    if (!parsed) {
      console.error(`❌ Invalid slug format: ${initialSlug}`);
      continue;
    }

    let currentSlug = initialSlug;
    if (!isSessionActive(initialSlug)) {
      // If the initial slug's session has expired, use current active session
      currentSlug = getCurrentSessionSlug(parsed.base);
      console.log(
        `⚠️  Initial slug ${initialSlug} session expired, switching to current session: ${currentSlug}`
      );
    }

    slugStates.push({
      base: parsed.base,
      currentSlug,
      websocket: null,
    });
  }

  if (slugStates.length === 0) {
    console.error("❌ No valid slugs to track");
    return;
  }

  console.log(`\n📊 Tracking ${slugStates.length} slug(s):`);
  slugStates.forEach((state) => {
    console.log(`   - ${state.currentSlug}`);
  });
  console.log();

  // Helper to convert Map to array and get wallet list
  const getWalletList = (
    wallets?: Map<string, string> | string[]
  ): string[] | undefined => {
    if (!wallets) return undefined;
    if (Array.isArray(wallets)) return wallets;
    return Array.from(wallets.keys());
  };

  // Helper to get wallet name
  const getWalletName = (
    wallet: string,
    wallets?: Map<string, string> | string[]
  ): string => {
    if (!wallets || Array.isArray(wallets)) return wallet;
    return wallets.get(wallet) || wallet;
  };

  const subscribeToSlug = (
    slug: string,
    wallets?: Map<string, string> | string[]
  ) => {
    const walletList = getWalletList(wallets);
    if (walletList && walletList.length > 0) {
      const walletNames = walletList
        .map((w) => getWalletName(w, wallets))
        .join(", ");
      console.log(
        `🔄 Subscribing to session: ${slug} (chỉ lưu ${walletList.length} ví: ${walletNames})`
      );
    } else {
      console.log(`🔄 Subscribing to session: ${slug} (lưu tất cả ví)`);
    }

    // Subscribe to slug (automatically categorizes by wallet)
    return subscribeOrdersMatched(slug, walletList, wallets);
  };

  const scheduleNextSession = (state: SlugState) => {
    const msUntilNext = getMsUntilNextSession(state.currentSlug);

    if (msUntilNext === Infinity) {
      console.error(
        `❌ Cannot parse slug format for ${state.currentSlug}, cannot auto-update`
      );
      return;
    }

    const nextSlug = getNextSessionSlug(state.currentSlug);
    if (!nextSlug) {
      console.error(
        `❌ Cannot generate next session slug for ${state.currentSlug}`
      );
      return;
    }

    const minutesUntilNext = Math.floor(msUntilNext / 60000);
    const secondsUntilNext = Math.floor((msUntilNext % 60000) / 1000);

    console.log(
      `⏰ Next session "${nextSlug}" starts in ${minutesUntilNext}m ${secondsUntilNext}s`
    );

    setTimeout(() => {
      console.log(`\n🔄 Switching to next session: ${nextSlug}`);

      // Close previous connection if exists
      if (state.websocket) {
        state.websocket.close();
      }

      // Subscribe to new session
      state.currentSlug = nextSlug;
      state.websocket = subscribeToSlug(state.currentSlug, walletsToTrack);

      // Schedule the next session switch for this slug
      scheduleNextSession(state);
    }, msUntilNext);
  };

  // ---- Subscribe realtime orders_matched for all slugs ----
  slugStates.forEach((state) => {
    state.websocket = subscribeToSlug(state.currentSlug, walletsToTrack);
    // Schedule automatic session updates for each slug
    scheduleNextSession(state);
  });

  // ---- (Optional) subscribe orderbook by asset ids if you need it ----
  // const assetIds = await getAssetIdsBySlug(slug);
  // subscribeMarket(assetIds);
}

main().catch(console.error);

async function appendTrades(
  filePath: string,
  trades: TradeItem[],
  ctx: { slug: string }
) {
  if (!trades.length) return;
  await fs.mkdir(path.dirname(filePath), { recursive: true });

  const lines = trades.map((t) =>
    JSON.stringify({
      receivedAt: new Date().toISOString(),
      eventSlug: ctx.slug,
      wallet: t.proxyWallet,
      side: t.side,
      size: t.size,
      price: t.price,
      outcome: t.outcome,
      outcomeIndex: t.outcomeIndex,
      onChainTimestamp: t.timestamp,
      transactionHash: t.transactionHash,
      conditionId: t.conditionId,
    })
  );

  await fs.appendFile(filePath, lines.join("\n") + "\n");
}

async function backfillAllTrades({
  conditionId,
  wallet,
  pageSize,
  filePath,
  slug,
}: {
  conditionId?: string;
  wallet?: string;
  pageSize: number;
  filePath: string;
  slug: string;
}) {
  let offset = 0;
  let total = 0;

  console.log(
    "backfillAllTrades",
    conditionId,
    wallet,
    pageSize,
    filePath,
    slug
  );

  while (true) {
    const batch = await fetchTrades({
      conditionId,
      wallet,
      limit: pageSize,
      offset,
    });
    console.log("batch", batch);

    if (!batch.length) break;

    await appendTrades(filePath, batch, { slug });
    total += batch.length;
    offset += pageSize;

    if (batch.length < pageSize) break; // last page
  }

  return total;
}
