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
  const initialSlug = "btc-updown-15m-1766058300"; // TODO: change to current session
  const walletToTrack = "0x6031B6eed1C97e853c6e0F03Ad3ce3529351F96d"; // TODO: change to wallet to track

  // Determine current active session
  let currentSlug = initialSlug;
  const parsed = parseSlug(initialSlug);
  if (parsed && !isSessionActive(initialSlug)) {
    // If the initial slug's session has expired, use current active session
    currentSlug = getCurrentSessionSlug(parsed.base);
    console.log(
      `⚠️  Initial slug session expired, switching to current session: ${currentSlug}`
    );
  }

  let currentWebSocket: ReturnType<typeof subscribeOrdersMatched> | null = null;

  const subscribeToSlug = (slug: string, wallet?: string) => {
    console.log(
      `🔄 Subscribing to session: ${slug}${
        wallet ? ` (tracking wallet: ${wallet})` : " (tracking all wallets)"
      }`
    );

    // Close previous connection if exists
    if (currentWebSocket) {
      currentWebSocket.close();
    }

    // Subscribe to new slug with wallet filter
    currentWebSocket = subscribeOrdersMatched(slug, wallet);
    return currentWebSocket;
  };

  const scheduleNextSession = () => {
    const msUntilNext = getMsUntilNextSession(currentSlug);

    if (msUntilNext === Infinity) {
      console.error("❌ Cannot parse slug format, cannot auto-update");
      return;
    }

    const nextSlug = getNextSessionSlug(currentSlug);
    if (!nextSlug) {
      console.error("❌ Cannot generate next session slug");
      return;
    }

    const minutesUntilNext = Math.floor(msUntilNext / 60000);
    const secondsUntilNext = Math.floor((msUntilNext % 60000) / 1000);

    console.log(
      `⏰ Next session "${nextSlug}" starts in ${minutesUntilNext}m ${secondsUntilNext}s`
    );

    setTimeout(() => {
      console.log(`\n🔄 Switching to next session: ${nextSlug}`);
      currentSlug = nextSlug;
      subscribeToSlug(currentSlug, walletToTrack);

      // Schedule the next session switch
      scheduleNextSession();
    }, msUntilNext);
  };

  // // ---- Backfill history before websocket (optional but recommended) ----
  // try {
  //   const conditionId = await getConditionIdBySlug(currentSlug);
  //   const total = await backfillAllTrades({
  //     conditionId,
  //     filePath: ordersFile,
  //     slug: currentSlug,
  //     pageSize,
  //   });
  //   console.log(`Backfilled ${total} trades to ${ordersFile}`);
  // } catch (err) {
  //   console.error("Failed to fetch historical trades", err);
  // }

  // ---- Subscribe realtime orders_matched ----
  subscribeToSlug(currentSlug, walletToTrack);

  // Schedule automatic session updates
  scheduleNextSession();

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
