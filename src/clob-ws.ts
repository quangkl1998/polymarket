import WebSocket from "ws";
import { promises as fs } from "fs";
import path from "path";

export function subscribeMarket(assetIds: string[]) {
  const ws = new WebSocket(
    "wss://ws-subscriptions-clob.polymarket.com/ws/market"
  );

  ws.on("open", () => {
    console.log("✅ Connected to Polymarket CLOB WSS");

    ws.send(
      JSON.stringify({
        type: "market",
        assets_ids: assetIds,
      })
    );
  });

  ws.on("message", (data) => {
    const msg = JSON.parse(data.toString());
    console.log("msg", msg);

    switch (msg.type) {
      case "book":
        console.log("📘 ORDERBOOK SNAPSHOT", msg);
        break;

      case "book_delta":
        console.log("📗 ORDERBOOK DELTA", msg);
        break;

      case "trade":
        console.log("💥 TRADE", msg);
        break;

      default:
        // ping / pong / other system messages
        break;
    }
  });

  ws.on("close", () => {
    console.log("❌ WebSocket closed");
  });

  ws.on("error", (err) => {
    console.error("WebSocket error:", err);
  });

  return ws;
}

/**
 * Convert normalized trade data to CSV row
 */
function toCsvRow(data: {
  receivedAt: string;
  eventSlug?: string;
  wallet?: string;
  side?: string;
  size?: number;
  price?: number;
  outcome?: string;
  outcomeIndex?: number;
  onChainTimestamp?: number;
  transactionHash?: string;
}): string {
  const escapeCsv = (value: unknown): string => {
    if (value === null || value === undefined) return "";
    const str = String(value);
    if (str.includes(",") || str.includes('"') || str.includes("\n")) {
      return `"${str.replace(/"/g, '""')}"`;
    }
    return str;
  };

  return [
    escapeCsv(data.receivedAt),
    escapeCsv(data.eventSlug),
    escapeCsv(data.wallet),
    escapeCsv(data.side),
    escapeCsv(data.size),
    escapeCsv(data.price),
    escapeCsv(data.outcome),
    escapeCsv(data.outcomeIndex),
    escapeCsv(data.onChainTimestamp),
    escapeCsv(data.transactionHash),
  ].join(",");
}

/**
 * CSV header row
 */
const CSV_HEADER =
  "receivedAt,eventSlug,wallet,side,size,price,outcome,outcomeIndex,onChainTimestamp,transactionHash";

/**
 * Get CSV file path for a session slug
 */
function getCsvFilePath(eventSlug: string): string {
  return path.join(process.cwd(), "data", "sessions", `${eventSlug}.csv`);
}

/**
 * Check if CSV file exists and has header
 * Returns true if file was just created, false if it already existed
 */
async function ensureCsvHeader(csvFile: string): Promise<boolean> {
  try {
    await fs.access(csvFile);
    // File exists, check if it has content
    const content = await fs.readFile(csvFile, "utf-8");
    if (!content.trim().startsWith(CSV_HEADER)) {
      // File exists but no header, prepend header
      const newContent = CSV_HEADER + "\n" + content;
      await fs.writeFile(csvFile, newContent);
    }
    return false; // File already existed
  } catch {
    // File doesn't exist, create with header
    await fs.mkdir(path.dirname(csvFile), { recursive: true });
    await fs.writeFile(csvFile, CSV_HEADER + "\n");
    return true; // File was just created
  }
}

/**
 * Subscribe to order matched feed for a given event slug.
 * Example slug: btc-updown-15m-1765785600
 * @param eventSlug - The event slug to subscribe to
 * @param walletToTrack - Optional wallet address to filter trades. If provided, only trades from this wallet will be persisted.
 */
export function subscribeOrdersMatched(
  eventSlug: string,
  walletToTrack?: string
) {
  const ws = new WebSocket("wss://ws-live-data.polymarket.com/");
  const ordersFile = path.join(process.cwd(), "data", "orders_matched.jsonl");
  const csvFile = getCsvFilePath(eventSlug);

  // Track if CSV header has been ensured
  let csvHeaderEnsured = false;

  const persistMessage = async (payload: unknown) => {
    try {
      await fs.mkdir(path.dirname(ordersFile), { recursive: true });
      const normalized = normalizeOrdersMatched(payload);
      if (!normalized) return;

      // Write to JSONL
      await fs.appendFile(ordersFile, JSON.stringify(normalized) + "\n");

      // Write to CSV (ensure header only once)
      if (!csvHeaderEnsured) {
        await ensureCsvHeader(csvFile);
        csvHeaderEnsured = true;
      }
      const csvRow = toCsvRow(normalized);
      await fs.appendFile(csvFile, csvRow + "\n");
    } catch (err) {
      console.error("Failed to write orders_matched", err);
    }
  };

  ws.on("open", () => {
    console.log("✅ Connected to Polymarket live-data WSS");

    const subPayload = {
      action: "subscribe",
      subscriptions: [
        {
          topic: "activity",
          type: "orders_matched",
          filters: JSON.stringify({ event_slug: eventSlug }),
        },
      ],
    };

    ws.send(JSON.stringify(subPayload));
  });

  ws.on("message", (data) => {
    const raw = data.toString();
    if (!raw.trim()) {
      // ignore empty frames
      return;
    }

    let msg: unknown;
    try {
      msg = JSON.parse(raw);
    } catch (err) {
      console.error("Failed to parse message", err, "raw:", raw);
      return;
    }

    const obj = msg as {
      type?: string;
      topic?: string;
      payload?: { proxyWallet?: string };
    };

    // Check if message is orders_matched
    const isOrdersMatched =
      obj?.type === "orders_matched" || obj?.topic === "activity";

    if (!isOrdersMatched) return;

    // If walletToTrack is provided, filter by wallet; otherwise persist all trades
    if (walletToTrack) {
      const matchesWallet =
        obj?.payload?.proxyWallet?.toLowerCase() ===
        walletToTrack.toLowerCase();
      if (matchesWallet) {
        console.log("💥 orders_matched", msg);
        void persistMessage(msg);
      }
    } else {
      // No wallet filter - persist all trades
      console.log("💥 orders_matched", msg);
      void persistMessage(msg);
    }
  });

  ws.on("close", () => {
    console.log("❌ Live-data WebSocket closed");
  });

  ws.on("error", (err) => {
    console.error("Live-data WebSocket error:", err);
  });

  return ws;
}

/**
 * Pick only needed fields for analysis.
 */
function normalizeOrdersMatched(payload: unknown):
  | {
      receivedAt: string;
      eventSlug?: string;
      wallet?: string;
      side?: string;
      size?: number;
      price?: number;
      outcome?: string;
      outcomeIndex?: number;
      onChainTimestamp?: number;
      transactionHash?: string;
    }
  | undefined {
  if (
    typeof payload !== "object" ||
    payload === null ||
    Array.isArray(payload)
  ) {
    return undefined;
  }

  const p = payload as {
    payload?: {
      eventSlug?: string;
      proxyWallet?: string;
      side?: string;
      size?: number;
      price?: number;
      outcome?: string;
      outcomeIndex?: number;
      timestamp?: number;
      transactionHash?: string;
    };
    timestamp?: number;
  };

  const inner = p.payload;
  if (!inner) return undefined;

  return {
    receivedAt: new Date().toISOString(),
    eventSlug: inner.eventSlug,
    wallet: inner.proxyWallet,
    side: inner.side,
    size: inner.size,
    price: inner.price,
    outcome: inner.outcome,
    outcomeIndex: inner.outcomeIndex,
    onChainTimestamp: inner.timestamp ?? p.timestamp,
    transactionHash: inner.transactionHash,
  };
}

/**
 * Track trades for a specific wallet. Writes filtered trades to data/wallets/<wallet>.jsonl
 */
export function trackWalletTrades(eventSlug: string, wallet: string) {
  const ws = subscribeOrdersMatched(eventSlug);
  const walletFile = path.join(
    process.cwd(),
    "data",
    "wallets",
    `${wallet}.jsonl`
  );

  const persistWallet = async (payload: unknown) => {
    const normalized = normalizeOrdersMatched(payload);
    if (
      !normalized ||
      normalized.wallet?.toLowerCase() !== wallet.toLowerCase()
    ) {
      return;
    }
    try {
      await fs.mkdir(path.dirname(walletFile), { recursive: true });
      await fs.appendFile(walletFile, JSON.stringify(normalized) + "\n");
    } catch (err) {
      console.error("Failed to write wallet trade", err);
    }
  };

  // Hook into the existing socket's message event
  ws.on("message", (data) => {
    const raw = data.toString();
    if (!raw.trim()) return;

    try {
      const parsed = JSON.parse(raw);
      void persistWallet(parsed);
    } catch (err) {
      // ignore parse errors here; already handled upstream
    }
  });

  return ws;
}
