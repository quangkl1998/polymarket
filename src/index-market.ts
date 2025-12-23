import { getAssetIdsBySlug } from "./polymarket";
import { subscribeMarket } from "./clob-ws";

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

  console.log("📅 Current session slugs (Market Data):");
  initialSlugs.forEach((slug, idx) => {
    console.log(`   ${idx + 1}. ${slug} (from base: ${basePatterns[idx]})`);
  });

  // Process each slug and determine current active sessions
  interface SlugState {
    base: string;
    currentSlug: string;
    marketWebsocket: ReturnType<typeof subscribeMarket> | null;
    assetIds: string[] | null;
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
      marketWebsocket: null,
      assetIds: null,
    });
  }

  if (slugStates.length === 0) {
    console.error("❌ No valid slugs to track");
    return;
  }

  console.log(`\n📊 Tracking ${slugStates.length} slug(s) for Market Data:`);
  slugStates.forEach((state) => {
    console.log(`   - ${state.currentSlug}`);
  });
  console.log();

  const subscribeToSlug = async (
    slug: string,
    state: SlugState,
    retryCount = 0
  ) => {
    console.log(
      `🔄 Subscribing to market data for session: ${slug}${
        retryCount > 0 ? ` (retry ${retryCount})` : ""
      }`
    );

    try {
      // Always fetch asset IDs for new session (don't use cached ones)
      console.log(`📊 Fetching asset IDs for ${slug}...`);
      let assetIds: string[] | null = null;

      try {
        assetIds = await getAssetIdsBySlug(slug);
        console.log(`✅ Got ${assetIds.length} asset IDs for ${slug}`);
        state.assetIds = assetIds; // Update state
      } catch (err: any) {
        // Market might not exist yet or API error
        const errorMsg =
          err?.response?.status === 404
            ? "Market not found (404)"
            : err?.message || "Unknown error";
        console.warn(`⚠️  Could not fetch asset IDs for ${slug}: ${errorMsg}.`);

        // Retry up to 3 times with exponential backoff
        if (retryCount < 3) {
          const delay = Math.pow(2, retryCount) * 2000; // 2s, 4s, 8s
          console.log(`   Retrying in ${delay / 1000}s...`);
          await new Promise((resolve) => setTimeout(resolve, delay));
          return subscribeToSlug(slug, state, retryCount + 1);
        }

        console.warn(
          `   Max retries reached. Skipping market subscription for ${slug}.`
        );
        state.assetIds = null;
        state.marketWebsocket = null;
        return null;
      }

      // Subscribe to market
      if (assetIds && assetIds.length > 0) {
        console.log(`📈 Subscribing to market data for ${slug}...`);
        // Close existing market websocket if any
        if (state.marketWebsocket) {
          console.log(
            `   Closing existing market websocket before subscribing to new one...`
          );
          try {
            state.marketWebsocket.close();
          } catch (err) {
            console.warn(`   Error closing existing websocket:`, err);
          }
          state.marketWebsocket = null;
        }

        // Wait a bit to ensure old connection is closed
        await new Promise((resolve) => setTimeout(resolve, 200));

        state.marketWebsocket = subscribeMarket(assetIds, slug);
        console.log(`   ✅ Market websocket created for ${slug}`);
        return state.marketWebsocket;
      } else {
        console.log(
          `⚠️  No asset IDs available for ${slug}, skipping market subscription`
        );
        // Ensure marketWebsocket is null if we're not subscribing
        state.marketWebsocket = null;
        return null;
      }
    } catch (err) {
      console.error(`❌ Failed to subscribe market for ${slug}:`, err);
      // Retry if not max retries
      if (retryCount < 3) {
        const delay = Math.pow(2, retryCount) * 2000;
        console.log(`   Retrying in ${delay / 1000}s...`);
        await new Promise((resolve) => setTimeout(resolve, delay));
        return subscribeToSlug(slug, state, retryCount + 1);
      }
      state.marketWebsocket = null;
      return null;
    }
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

    setTimeout(async () => {
      console.log(`\n🔄 Switching to next session: ${nextSlug}`);

      try {
        // Close previous connections if exist
        if (state.marketWebsocket) {
          console.log(`   Closing previous market websocket...`);
          try {
            state.marketWebsocket.close();
          } catch (err) {
            console.warn(`   Error closing market websocket:`, err);
          }
          state.marketWebsocket = null;
        }

        // Wait a bit to ensure connections are closed
        await new Promise((resolve) => setTimeout(resolve, 500));

        // Subscribe to new session
        state.currentSlug = nextSlug;
        // Reset assetIds to fetch new ones for new session (important!)
        state.assetIds = null;

        console.log(`   Subscribing to new session: ${nextSlug}`);
        const newWebsocket = await subscribeToSlug(state.currentSlug, state);
        state.marketWebsocket = newWebsocket;

        // Verify market subscription
        if (state.marketWebsocket) {
          console.log(`   ✅ Market subscription active for ${nextSlug}`);
          // Schedule the next session switch for this slug
          scheduleNextSession(state);
        } else {
          console.log(`   ⚠️  Market subscription not active for ${nextSlug}`);
          // Still schedule next session even if subscription failed
          // This allows retry on next session
          console.log(`   ⏰ Will retry subscription on next session`);
          scheduleNextSession(state);
        }
      } catch (err) {
        console.error(`❌ Error switching to next session ${nextSlug}:`, err);
        // Try to schedule again after a delay
        setTimeout(() => {
          console.log(`   Retrying session switch for ${nextSlug}...`);
          scheduleNextSession(state);
        }, 5000);
      }
    }, msUntilNext);
  };

  // ---- Subscribe realtime market data for all slugs ----
  for (const state of slugStates) {
    state.marketWebsocket = await subscribeToSlug(state.currentSlug, state);
    // Schedule automatic session updates for each slug
    scheduleNextSession(state);
  }
}

main().catch(console.error);
