import WebSocket from "ws";
import { promises as fs } from "fs";
import path from "path";
import * as XLSX from "xlsx";

/**
 * Subscribe to market data (orderbook, trades) for a slug
 * Saves data to Excel file: data/markets/{slug}.xlsx
 * Each message type (book, book_delta, trade) is saved to separate sheets
 */
export function subscribeMarket(assetIds: string[], slug: string) {
  const ws = new WebSocket(
    "wss://ws-subscriptions-clob.polymarket.com/ws/market"
  );

  // Excel file path: data/markets/{slug}.xlsx
  const excelFile = path.join(process.cwd(), "data", "markets", `${slug}.xlsx`);

  // JSONL file path: data/markets/{slug}.jsonl
  const jsonlFile = path.join(
    process.cwd(),
    "data",
    "markets",
    `${slug}.jsonl`
  );

  // Cache workbook
  let workbook: XLSX.WorkBook | null = null;

  // Helper to get or create workbook
  const getWorkbook = async (): Promise<XLSX.WorkBook> => {
    if (workbook) return workbook;

    try {
      await fs.access(excelFile);
      workbook = XLSX.readFile(excelFile);
    } catch {
      workbook = XLSX.utils.book_new();
    }
    return workbook;
  };

  // Helper to append data to JSONL file
  const appendToJsonl = async (data: any) => {
    try {
      const jsonlData = {
        receivedAt: new Date().toISOString(),
        slug: slug,
        ...data,
      };
      const jsonlLine = JSON.stringify(jsonlData) + "\n";
      await fs.mkdir(path.dirname(jsonlFile), { recursive: true });
      await fs.appendFile(jsonlFile, jsonlLine, "utf-8");
    } catch (err) {
      console.error(`Failed to write JSONL for ${slug}:`, err);
    }
  };

  // Helper to append data to sheet
  const appendToSheet = async (
    sheetName: string,
    data: any,
    headers: string[]
  ) => {
    const wb = await getWorkbook();

    // Get or create sheet
    let worksheet: XLSX.WorkSheet;
    if (wb.SheetNames.includes(sheetName)) {
      worksheet = wb.Sheets[sheetName];
    } else {
      worksheet = XLSX.utils.aoa_to_sheet([headers]);
      XLSX.utils.book_append_sheet(wb, worksheet, sheetName);
    }

    // Convert data to row format
    const row: any = {
      timestamp: new Date().toISOString(),
      ...data,
    };

    // Append row
    XLSX.utils.sheet_add_json(worksheet, [row], {
      origin: -1,
      skipHeader: true,
    });

    // Save workbook
    await fs.mkdir(path.dirname(excelFile), { recursive: true });
    XLSX.writeFile(wb, excelFile);
  };

  ws.on("open", () => {
    console.log(`✅ Connected to Polymarket CLOB WSS for ${slug}`);

    ws.send(
      JSON.stringify({
        type: "market",
        assets_ids: assetIds,
      })
    );
  });

  ws.on("message", async (data) => {
    try {
      const msg = JSON.parse(data.toString());

      switch (msg.event_type) {
        case "book":
          // Save to Excel
          await appendToSheet(
            "orderbook_snapshot",
            {
              asset_id: msg.asset_id || "",
              bids: JSON.stringify(msg.bids || []),
              asks: JSON.stringify(msg.asks || []),
              raw_data: JSON.stringify(msg),
            },
            ["timestamp", "asset_id", "bids", "asks", "raw_data"]
          );
          // Save to JSONL
          await appendToJsonl({
            type: "orderbook_snapshot",
            asset_id: msg.asset_id,
            bids: msg.bids,
            asks: msg.asks,
            raw_data: msg,
          });
          break;

        case "book_delta":
          // Save to Excel
          await appendToSheet(
            "orderbook_delta",
            {
              asset_id: msg.asset_id || "",
              bids: JSON.stringify(msg.bids || []),
              asks: JSON.stringify(msg.asks || []),
              raw_data: JSON.stringify(msg),
            },
            ["timestamp", "asset_id", "bids", "asks", "raw_data"]
          );
          // Save to JSONL
          await appendToJsonl({
            type: "orderbook_delta",
            asset_id: msg.asset_id,
            bids: msg.bids,
            asks: msg.asks,
            raw_data: msg,
          });
          break;

        case "trade":
          // Save to Excel
          await appendToSheet(
            "trades",
            {
              asset_id: msg.asset_id || "",
              price: msg.price || "",
              size: msg.size || "",
              side: msg.side || "",
              maker: msg.maker || "",
              taker: msg.taker || "",
              raw_data: JSON.stringify(msg),
            },
            [
              "timestamp",
              "asset_id",
              "price",
              "size",
              "side",
              "maker",
              "taker",
              "raw_data",
            ]
          );
          // Save to JSONL
          await appendToJsonl({
            type: "trade",
            asset_id: msg.asset_id,
            price: msg.price,
            size: msg.size,
            side: msg.side,
            maker: msg.maker,
            taker: msg.taker,
            raw_data: msg,
          });
          break;

        case "price_change":
          // Save each price change as a separate row
          if (msg.price_changes && Array.isArray(msg.price_changes)) {
            for (const priceChange of msg.price_changes) {
              // Save to Excel
              await appendToSheet(
                "price_changes",
                {
                  market: msg.market || "",
                  asset_id: priceChange.asset_id || "",
                  price: priceChange.price || "",
                  size: priceChange.size || "",
                  side: priceChange.side || "",
                  hash: priceChange.hash || "",
                  best_bid: priceChange.best_bid || "",
                  best_ask: priceChange.best_ask || "",
                  raw_data: JSON.stringify(priceChange),
                },
                [
                  "timestamp",
                  "market",
                  "asset_id",
                  "price",
                  "size",
                  "side",
                  "hash",
                  "best_bid",
                  "best_ask",
                  "raw_data",
                ]
              );
            }
          }
          // Save to JSONL (full message)
          await appendToJsonl({
            type: "price_change",
            market: msg.market,
            price_changes: msg.price_changes,
            raw_data: msg,
          });
          break;

        default:
          // ping / pong / other system messages
          break;
      }
    } catch (err) {
      console.error(`Failed to process market message for ${slug}:`, err);
    }
  });

  ws.on("close", () => {
    console.log(`❌ Market WebSocket closed for ${slug}`);
  });

  ws.on("error", (err) => {
    console.error(`Market WebSocket error for ${slug}:`, err);
  });

  return ws;
}

/**
 * Extract market name from slug
 * Example: "btc-updown-15m-1766147400" -> "btc"
 *          "eth-updown-15m-1766147400" -> "eth"
 */
function extractMarketFromSlug(slug: string): string {
  // Extract the first part before the first dash (market name)
  const match = slug.match(/^([^-]+)/);
  return match ? match[1].toLowerCase() : "unknown";
}

/**
 * Extract date string from timestamp or ISO string
 * Returns format: YYYY-MM-DD
 */
function extractDateString(
  receivedAt?: string,
  onChainTimestamp?: number
): string {
  let date: Date;

  if (receivedAt) {
    // Try to parse ISO string
    date = new Date(receivedAt);
  } else if (onChainTimestamp) {
    // Convert Unix timestamp to Date (assuming seconds, not milliseconds)
    date = new Date(onChainTimestamp * 1000);
  } else {
    // Fallback to current date
    date = new Date();
  }

  // Format as YYYY-MM-DD
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, "0");
  const day = String(date.getUTCDate()).padStart(2, "0");

  return `${year}-${month}-${day}`;
}

/**
 * Sanitize wallet name for use as folder name
 * Remove invalid characters and replace with underscores
 */
function sanitizeFolderName(name: string): string {
  return name
    .replace(/[^a-zA-Z0-9_-]/g, "_")
    .replace(/_{2,}/g, "_")
    .replace(/^_|_$/g, "");
}

/**
 * Get wallet folder name from address or name map
 */
function getWalletFolderName(
  wallet: string,
  walletNameMap?: Map<string, string>
): string {
  const walletLower = wallet.toLowerCase();
  const name = walletNameMap?.get(walletLower);
  if (name) {
    return sanitizeFolderName(name);
  }
  // Fallback to wallet address if no name provided
  return walletLower;
}

/**
 * Get CSV file path for a session slug and wallet
 * Structure: data/wallets/<wallet-name>/<market>/<session-slug>.csv
 * Example: data/wallets/Wallet_1/btc/btc-updown-15m-1766147400.csv
 */
function getCsvFilePath(
  eventSlug: string,
  wallet: string,
  walletNameMap?: Map<string, string>
): string {
  const market = extractMarketFromSlug(eventSlug);
  const walletFolder = getWalletFolderName(wallet, walletNameMap);
  return path.join(
    process.cwd(),
    "data",
    "wallets",
    walletFolder,
    market,
    `${eventSlug}.csv`
  );
}

/**
 * Save wallet address to name mapping in metadata file
 */
async function saveWalletMapping(
  walletFolder: string,
  walletAddress: string,
  walletName?: string
): Promise<void> {
  const metadataFile = path.join(walletFolder, ".wallet.json");
  const metadata = {
    address: walletAddress,
    name: walletName || walletAddress,
    updatedAt: new Date().toISOString(),
  };
  try {
    await fs.mkdir(walletFolder, { recursive: true });
    await fs.writeFile(
      metadataFile,
      JSON.stringify(metadata, null, 2),
      "utf-8"
    );
  } catch (err) {
    console.error(`Failed to save wallet mapping: ${err}`);
  }
}

/**
 * Subscribe to order matched feed for a given event slug.
 * Automatically categorizes trades by wallet into Excel files, organized by date.
 * Data structure: data/wallets/{wallet-name}/{market}/{YYYY-MM-DD}.xlsx
 * Each file represents one day, with each session as a separate sheet within that file.
 * Example slug: btc-updown-15m-1765785600
 * @param eventSlug - The event slug to subscribe to
 * @param wallets - Optional list of wallet addresses to filter. If provided, only trades from these wallets will be saved.
 * @param walletNames - Optional Map of wallet addresses to custom names for display/logging
 */
export function subscribeOrdersMatched(
  eventSlug: string,
  wallets?: string[],
  walletNames?: Map<string, string> | string[]
) {
  const ws = new WebSocket("wss://ws-live-data.polymarket.com/");

  // Track which wallets have had their metadata saved
  const walletsMetadataSaved = new Set<string>();

  // Cache for Excel workbooks (wallet_market -> workbook)
  const workbookCache = new Map<string, XLSX.WorkBook>();

  // Create a Set of allowed wallets (lowercase) for fast lookup
  const allowedWallets = wallets
    ? new Set(wallets.map((w) => w.toLowerCase()))
    : null;

  // Create a Map for wallet names (lowercase key -> name)
  const walletNameMap = new Map<string, string>();
  if (walletNames) {
    if (Array.isArray(walletNames)) {
      // If array, use wallet as name
      walletNames.forEach((w) => {
        walletNameMap.set(w.toLowerCase(), w);
      });
    } else {
      // If Map, use the provided names
      walletNames.forEach((name, wallet) => {
        walletNameMap.set(wallet.toLowerCase(), name);
      });
    }
  }

  // Helper to get wallet display name
  const getWalletDisplayName = (wallet: string): string => {
    const name = walletNameMap.get(wallet.toLowerCase());
    return name ? `${name} (${wallet})` : wallet;
  };

  if (allowedWallets) {
    const walletList = Array.from(allowedWallets)
      .map((w) => getWalletDisplayName(w))
      .join(", ");
    console.log(
      `🔍 Chỉ lưu giao dịch từ ${allowedWallets.size} ví được chỉ định: ${walletList}`
    );
  }

  const persistMessage = async (payload: unknown) => {
    try {
      const normalized = normalizeOrdersMatched(payload);
      if (!normalized || !normalized.wallet) {
        return;
      }

      const wallet = normalized.wallet.toLowerCase();

      // Filter by wallet list if provided
      if (allowedWallets && !allowedWallets.has(wallet)) {
        // Wallet not in the allowed list, skip
        return;
      }

      // Get event slug from normalized data or use the one from function scope
      const currentEventSlug = normalized.eventSlug || eventSlug;
      if (!currentEventSlug) {
        return;
      }

      // Get wallet folder name and save mapping
      const walletFolderName = getWalletFolderName(wallet, walletNameMap);
      const walletName = walletNameMap.get(wallet.toLowerCase());
      const walletFolder = path.join(
        process.cwd(),
        "data",
        "wallets",
        walletFolderName
      );

      // Save wallet mapping metadata (only once per wallet)
      if (!walletsMetadataSaved.has(wallet)) {
        await saveWalletMapping(walletFolder, wallet, walletName);
        walletsMetadataSaved.add(wallet);
      }

      // Get market from event slug
      const market = extractMarketFromSlug(currentEventSlug);

      // Get date string for file organization
      const dateString = extractDateString(
        normalized.receivedAt,
        normalized.onChainTimestamp
      );

      // Excel file path: data/wallets/{wallet-name}/{market}/{YYYY-MM-DD}.xlsx
      // One file per day, with each session as a separate sheet
      const marketFolder = path.join(walletFolder, market);
      const excelFile = path.join(marketFolder, `${dateString}.xlsx`);
      const cacheKey = `${wallet}_${market}_${dateString}`;

      // Get or create workbook for this wallet + market + date
      let workbook: XLSX.WorkBook;
      if (workbookCache.has(cacheKey)) {
        workbook = workbookCache.get(cacheKey)!;
      } else {
        // Try to load existing file
        try {
          await fs.mkdir(marketFolder, { recursive: true });
          await fs.access(excelFile);
          workbook = XLSX.readFile(excelFile);
        } catch {
          // File doesn't exist, create new workbook
          workbook = XLSX.utils.book_new();
        }
        workbookCache.set(cacheKey, workbook);
      }

      // Prepare row data with wallet name
      const rowData = {
        receivedAt: normalized.receivedAt || "",
        eventSlug: currentEventSlug,
        wallet: normalized.wallet || wallet,
        walletName: walletName || wallet,
        walletAddress: wallet,
        side: normalized.side || "",
        size: normalized.size || "",
        price: normalized.price || "",
        outcome: normalized.outcome || "",
        outcomeIndex: normalized.outcomeIndex || "",
        onChainTimestamp: normalized.onChainTimestamp || "",
        transactionHash: normalized.transactionHash || "",
      };

      // Get or create sheet for this session
      // Sheet name format: session slug (e.g., btc-updown-15m-1766147400)
      // Each session in the same day will be a separate sheet in the same file
      const sessionTimestamp =
        currentEventSlug.split("-").pop() || currentEventSlug;
      const sheetName = currentEventSlug
        .substring(0, 31)
        .replace(/[\\\/\?\*\[\]]/g, "_");
      let worksheet: XLSX.WorkSheet;

      if (workbook.SheetNames.includes(sheetName)) {
        // Sheet exists, get it
        worksheet = workbook.Sheets[sheetName];
      } else {
        // Create new sheet with header
        const headers = [
          "receivedAt",
          "eventSlug",
          "wallet",
          "walletName",
          "walletAddress",
          "side",
          "size",
          "price",
          "outcome",
          "outcomeIndex",
          "onChainTimestamp",
          "transactionHash",
        ];
        worksheet = XLSX.utils.aoa_to_sheet([headers]);
        XLSX.utils.book_append_sheet(workbook, worksheet, sheetName);
      }

      // Append row to sheet
      XLSX.utils.sheet_add_json(worksheet, [rowData], {
        origin: -1, // Append to end
        skipHeader: true,
      });

      // Save workbook to file
      // marketFolder already exists from mkdir above
      await fs.mkdir(marketFolder, { recursive: true });
      XLSX.writeFile(workbook, excelFile);
    } catch (err) {
      console.error("Failed to write orders_matched", err);
      // Try to log error details if variables are available
      try {
        const normalized = normalizeOrdersMatched(payload);
        const wallet = normalized?.wallet?.toLowerCase() || "unknown";
        const currentEventSlug =
          normalized?.eventSlug || eventSlug || "unknown";
        const market = currentEventSlug
          ? extractMarketFromSlug(currentEventSlug)
          : "unknown";
        console.error("Error details:", {
          wallet,
          market,
          eventSlug: currentEventSlug,
        });
      } catch {
        // Ignore errors in error logging
      }
    }
  };

  ws.on("open", () => {
    console.log(`✅ Connected to Polymarket live-data WSS for ${eventSlug}`);

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

    console.log(`📡 Subscribing to orders_matched for event: ${eventSlug}`);
    ws.send(JSON.stringify(subPayload));
  });

  ws.on("message", (data) => {
    const raw = data.toString();
    if (!raw.trim()) {
      // ignore empty frames
      return;
    }

    let msg: any;
    try {
      msg = JSON.parse(raw);
    } catch (err) {
      console.error("Failed to parse message", err, "raw:", raw);
      return;
    }

    const obj = msg as {
      type?: string;
      topic?: string;
      payload?: { proxyWallet?: string; eventSlug?: string };
    };

    // Check if message is orders_matched
    const isOrdersMatched =
      obj?.type === "orders_matched" || obj?.topic === "activity";

    if (!isOrdersMatched) return;

    // Persist all trades (automatically categorized by wallet)
    const walletAddr = msg?.payload?.proxyWallet;

    // Only log if wallet is in the allowed list (if list is provided)
    // If no wallet list provided, log all
    const shouldLog =
      !allowedWallets ||
      (walletAddr && allowedWallets.has(walletAddr.toLowerCase()));

    if (shouldLog && walletAddr) {
      // Get wallet name (without address)
      const walletName =
        walletNameMap.get(walletAddr.toLowerCase()) || walletAddr;

      // Get market from event slug
      const currentEventSlug = msg?.payload?.eventSlug || eventSlug;
      const market = currentEventSlug
        ? extractMarketFromSlug(currentEventSlug)
        : "unknown";

      // Format time (Unix timestamp)
      const time = msg?.payload?.timestamp || "";

      console.log(
        `💥 Order matched: ${market} ${walletName} side: ${
          msg?.payload?.side
        } size: ${msg?.payload?.size} price: ${
          msg?.payload?.price?.toFixed(2) || "N/A"
        } time: ${time}`
      );
    }

    void persistMessage(msg);
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
