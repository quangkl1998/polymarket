import { promises as fs } from "fs";
import path from "path";

/**
 * Type definition for orders_matched data structure
 */
export interface OrderMatched {
  receivedAt: string;
  eventSlug?: string;
  wallet?: string;
  side?: "BUY" | "SELL";
  size?: number;
  price?: number;
  outcome?: string;
  outcomeIndex?: number;
  onChainTimestamp?: number;
  transactionHash?: string;
  conditionId?: string;
}

/**
 * Statistics for a wallet
 */
export interface WalletStats {
  wallet: string;
  totalTrades: number;
  buyCount: number;
  sellCount: number;
  totalBuyVolume: number;
  totalSellVolume: number;
  averageBuyPrice: number;
  averageSellPrice: number;
  totalVolume: number;
}

/**
 * Profit calculation result
 */
export interface WalletProfit {
  wallet: string;
  totalCost: number; // Tổng chi phí mua
  totalRevenue: number; // Tổng doanh thu bán
  realizedProfit: number; // Lợi nhuận đã thực hiện (đã đóng position)
  unrealizedProfit: number; // Lợi nhuận chưa thực hiện (còn position)
  totalProfit: number; // Tổng profit (realized + unrealized)
  openPosition: number; // Số lượng position còn mở (dương = đang nắm giữ, âm = đang short)
  profitByOutcome: Map<
    number,
    {
      outcomeIndex: number;
      outcome?: string;
      realizedProfit: number;
      openPosition: number;
      averageBuyPrice: number;
      averageSellPrice: number;
    }
  >;
}

/**
 * Read and parse JSONL file
 */
export async function readOrdersMatched(
  filePath: string
): Promise<OrderMatched[]> {
  const content = await fs.readFile(filePath, "utf-8");
  const lines = content
    .trim()
    .split("\n")
    .filter((line) => line.trim());

  return lines
    .map((line) => {
      try {
        return JSON.parse(line) as OrderMatched;
      } catch (err) {
        console.error(`Failed to parse line: ${line}`, err);
        return null;
      }
    })
    .filter((item): item is OrderMatched => item !== null);
}

/**
 * Calculate statistics for a specific wallet
 */
export function getWalletStats(
  orders: OrderMatched[],
  wallet: string
): WalletStats {
  const walletOrders = orders.filter(
    (o) => o.wallet?.toLowerCase() === wallet.toLowerCase()
  );

  const buyOrders = walletOrders.filter((o) => o.side === "BUY");
  const sellOrders = walletOrders.filter((o) => o.side === "SELL");

  const totalBuyVolume = buyOrders.reduce((sum, o) => sum + (o.size || 0), 0);
  const totalSellVolume = sellOrders.reduce((sum, o) => sum + (o.size || 0), 0);

  const totalBuyValue = buyOrders.reduce(
    (sum, o) => sum + (o.size || 0) * (o.price || 0),
    0
  );
  const totalSellValue = sellOrders.reduce(
    (sum, o) => sum + (o.size || 0) * (o.price || 0),
    0
  );

  const averageBuyPrice =
    totalBuyVolume > 0 ? totalBuyValue / totalBuyVolume : 0;
  const averageSellPrice =
    totalSellVolume > 0 ? totalSellValue / totalSellVolume : 0;

  return {
    wallet,
    totalTrades: walletOrders.length,
    buyCount: buyOrders.length,
    sellCount: sellOrders.length,
    totalBuyVolume,
    totalSellVolume,
    averageBuyPrice,
    averageSellPrice,
    totalVolume: totalBuyVolume + totalSellVolume,
  };
}

/**
 * Get all unique wallets from orders
 */
export function getAllWallets(orders: OrderMatched[]): string[] {
  const wallets = new Set<string>();
  orders.forEach((o) => {
    if (o.wallet) {
      wallets.add(o.wallet.toLowerCase());
    }
  });
  return Array.from(wallets);
}

/**
 * Get statistics for all wallets
 */
export function getAllWalletStats(
  orders: OrderMatched[]
): Map<string, WalletStats> {
  const wallets = getAllWallets(orders);
  const statsMap = new Map<string, WalletStats>();

  wallets.forEach((wallet) => {
    statsMap.set(wallet, getWalletStats(orders, wallet));
  });

  return statsMap;
}

/**
 * Get top wallets by trade count
 */
export function getTopWalletsByTradeCount(
  orders: OrderMatched[],
  limit: number = 10
): WalletStats[] {
  const statsMap = getAllWalletStats(orders);
  return Array.from(statsMap.values())
    .sort((a, b) => b.totalTrades - a.totalTrades)
    .slice(0, limit);
}

/**
 * Get top wallets by volume
 */
export function getTopWalletsByVolume(
  orders: OrderMatched[],
  limit: number = 10
): WalletStats[] {
  const statsMap = getAllWalletStats(orders);
  return Array.from(statsMap.values())
    .sort((a, b) => b.totalVolume - a.totalVolume)
    .slice(0, limit);
}

/**
 * Calculate profit for a wallet using FIFO matching
 * Matches BUY orders with SELL orders by outcome and time
 */
export function calculateWalletProfit(
  orders: OrderMatched[],
  wallet: string,
  currentPrice?: Map<number, number> // Optional: current price by outcomeIndex for unrealized profit
): WalletProfit {
  const walletOrders = orders
    .filter((o) => o.wallet?.toLowerCase() === wallet.toLowerCase())
    .sort((a, b) => (a.onChainTimestamp || 0) - (b.onChainTimestamp || 0));

  const profitByOutcome = new Map<
    number,
    {
      outcomeIndex: number;
      outcome?: string;
      realizedProfit: number;
      openPosition: number;
      averageBuyPrice: number;
      averageSellPrice: number;
      buyQueue: Array<{ size: number; price: number }>;
    }
  >();

  // Process each order
  walletOrders.forEach((order) => {
    if (order.outcomeIndex === undefined) return;

    const outcomeIndex = order.outcomeIndex;
    if (!profitByOutcome.has(outcomeIndex)) {
      profitByOutcome.set(outcomeIndex, {
        outcomeIndex,
        outcome: order.outcome,
        realizedProfit: 0,
        openPosition: 0,
        averageBuyPrice: 0,
        averageSellPrice: 0,
        buyQueue: [],
      });
    }

    const outcomeData = profitByOutcome.get(outcomeIndex)!;
    const size = order.size || 0;
    const price = order.price || 0;

    if (order.side === "BUY") {
      outcomeData.buyQueue.push({ size, price });
      outcomeData.openPosition += size;
    } else if (order.side === "SELL") {
      let remainingSell = size;

      // Match with existing BUY orders (FIFO)
      while (remainingSell > 0 && outcomeData.buyQueue.length > 0) {
        const buyOrder = outcomeData.buyQueue[0];
        const matchedSize = Math.min(remainingSell, buyOrder.size);

        // Calculate profit: (sell_price - buy_price) * matched_size
        const profit = (price - buyOrder.price) * matchedSize;
        outcomeData.realizedProfit += profit;

        buyOrder.size -= matchedSize;
        remainingSell -= matchedSize;
        outcomeData.openPosition -= matchedSize;

        if (buyOrder.size <= 0) {
          outcomeData.buyQueue.shift();
        }
      }

      // If there's remaining sell that doesn't match any buy, it's a short position
      if (remainingSell > 0) {
        outcomeData.openPosition -= remainingSell;
      }
    }
  });

  // Calculate averages and prepare result
  const profitByOutcomeResult = new Map<
    number,
    {
      outcomeIndex: number;
      outcome?: string;
      realizedProfit: number;
      openPosition: number;
      averageBuyPrice: number;
      averageSellPrice: number;
    }
  >();

  let totalCost = 0;
  let totalRevenue = 0;
  let totalRealizedProfit = 0;
  let totalUnrealizedProfit = 0;
  let totalOpenPosition = 0;

  profitByOutcome.forEach((data, outcomeIndex) => {
    // Get all orders for this outcome to calculate totals
    const outcomeOrders = walletOrders.filter(
      (o) => o.outcomeIndex === outcomeIndex
    );
    const buyOrders = outcomeOrders.filter((o) => o.side === "BUY");
    const sellOrders = outcomeOrders.filter((o) => o.side === "SELL");

    // Calculate total cost (all BUY orders)
    const outcomeTotalCost = buyOrders.reduce(
      (sum, o) => sum + (o.size || 0) * (o.price || 0),
      0
    );
    // Calculate total revenue (all SELL orders)
    const outcomeTotalRevenue = sellOrders.reduce(
      (sum, o) => sum + (o.size || 0) * (o.price || 0),
      0
    );

    // Calculate average buy price from all buy orders
    const totalBuySize = buyOrders.reduce((sum, o) => sum + (o.size || 0), 0);
    data.averageBuyPrice =
      totalBuySize > 0 ? outcomeTotalCost / totalBuySize : 0;

    // Calculate average sell price from all sell orders
    const totalSellSize = sellOrders.reduce((sum, o) => sum + (o.size || 0), 0);
    data.averageSellPrice =
      totalSellSize > 0 ? outcomeTotalRevenue / totalSellSize : 0;

    // Calculate average buy price from remaining positions (for unrealized profit)
    const remainingBuyValue = data.buyQueue.reduce(
      (sum, b) => sum + b.size * b.price,
      0
    );
    const remainingBuySize = data.buyQueue.reduce((sum, b) => sum + b.size, 0);
    const avgRemainingBuyPrice =
      remainingBuySize > 0 ? remainingBuyValue / remainingBuySize : 0;

    // Calculate unrealized profit if current price is provided
    let unrealized = 0;
    if (
      currentPrice &&
      currentPrice.has(outcomeIndex) &&
      data.openPosition > 0
    ) {
      const current = currentPrice.get(outcomeIndex)!;
      unrealized = (current - avgRemainingBuyPrice) * data.openPosition;
    }

    totalCost += outcomeTotalCost;
    totalRevenue += outcomeTotalRevenue;
    totalRealizedProfit += data.realizedProfit;
    totalUnrealizedProfit += unrealized;
    totalOpenPosition += data.openPosition;

    profitByOutcomeResult.set(outcomeIndex, {
      outcomeIndex: data.outcomeIndex,
      outcome: data.outcome,
      realizedProfit: data.realizedProfit,
      openPosition: data.openPosition,
      averageBuyPrice: data.averageBuyPrice,
      averageSellPrice: data.averageSellPrice,
    });
  });

  return {
    wallet,
    totalCost,
    totalRevenue,
    realizedProfit: totalRealizedProfit,
    unrealizedProfit: totalUnrealizedProfit,
    totalProfit: totalRealizedProfit + totalUnrealizedProfit,
    openPosition: totalOpenPosition,
    profitByOutcome: profitByOutcomeResult,
  };
}

/**
 * Get top wallets by realized profit
 */
export function getTopWalletsByProfit(
  orders: OrderMatched[],
  limit: number = 10,
  currentPrice?: Map<number, number>
): Array<WalletProfit & { rank: number }> {
  const wallets = getAllWallets(orders);
  const profits = wallets.map((wallet) =>
    calculateWalletProfit(orders, wallet, currentPrice)
  );

  return profits
    .sort((a, b) => b.realizedProfit - a.realizedProfit)
    .slice(0, limit)
    .map((p, idx) => ({ ...p, rank: idx + 1 }));
}

/**
 * Statistics for a specific price level
 */
export interface PriceLevelStats {
  price: number;
  buyWallets: number; // Số ví đang mua
  sellWallets: number; // Số ví đang bán
  buyVolume: number; // Tổng khối lượng mua
  sellVolume: number; // Tổng khối lượng bán
  buyTrades: number; // Số lượng giao dịch mua
  sellTrades: number; // Số lượng giao dịch bán
  totalVolume: number; // Tổng khối lượng
  totalTrades: number; // Tổng số giao dịch
  wallets: Set<string>; // Tất cả ví tham gia
  outcomeIndex?: number; // Optional: filter by outcome
}

/**
 * Analyze trades by price level
 * Groups all trades by price and calculates statistics
 */
export function analyzeByPrice(
  orders: OrderMatched[],
  outcomeIndex?: number
): Map<number, PriceLevelStats> {
  // Filter by outcome if specified
  const filteredOrders =
    outcomeIndex !== undefined
      ? orders.filter((o) => o.outcomeIndex === outcomeIndex)
      : orders;

  const priceMap = new Map<
    number,
    {
      price: number;
      buyWallets: Set<string>;
      sellWallets: Set<string>;
      buyVolume: number;
      sellVolume: number;
      buyTrades: number;
      sellTrades: number;
      wallets: Set<string>;
      outcomeIndex?: number;
    }
  >();

  filteredOrders.forEach((order) => {
    if (order.price === undefined || order.price === null) return;
    if (!order.wallet) return;

    const price = order.price;
    if (!priceMap.has(price)) {
      priceMap.set(price, {
        price,
        buyWallets: new Set(),
        sellWallets: new Set(),
        buyVolume: 0,
        sellVolume: 0,
        buyTrades: 0,
        sellTrades: 0,
        wallets: new Set(),
        outcomeIndex,
      });
    }

    const stats = priceMap.get(price)!;
    const size = order.size || 0;
    const wallet = order.wallet.toLowerCase();

    stats.wallets.add(wallet);

    if (order.side === "BUY") {
      stats.buyVolume += size;
      stats.buyTrades += 1;
      stats.buyWallets.add(wallet);
    } else if (order.side === "SELL") {
      stats.sellVolume += size;
      stats.sellTrades += 1;
      stats.sellWallets.add(wallet);
    }
  });

  // Convert to final format
  const result = new Map<number, PriceLevelStats>();
  priceMap.forEach((data, price) => {
    result.set(price, {
      price: data.price,
      buyWallets: data.buyWallets.size,
      sellWallets: data.sellWallets.size,
      buyVolume: data.buyVolume,
      sellVolume: data.sellVolume,
      buyTrades: data.buyTrades,
      sellTrades: data.sellTrades,
      totalVolume: data.buyVolume + data.sellVolume,
      totalTrades: data.buyTrades + data.sellTrades,
      wallets: data.wallets,
      outcomeIndex: data.outcomeIndex,
    });
  });

  return result;
}

/**
 * Get statistics for a specific price
 */
export function getPriceStats(
  orders: OrderMatched[],
  price: number,
  outcomeIndex?: number
): PriceLevelStats | null {
  const priceMap = analyzeByPrice(orders, outcomeIndex);
  return priceMap.get(price) || null;
}

/**
 * Price snapshot at a specific time
 */
export interface PriceSnapshot {
  timestamp: number;
  price: number;
  outcomeIndex?: number;
  outcome?: string;
  volume: number;
  trades: number;
}

/**
 * Detailed price snapshot with separate buy/sell statistics
 */
export interface DetailedPriceSnapshot {
  timestamp: number;
  outcomeIndex?: number;
  outcome?: string;
  // Buy statistics
  buyPrice: number; // Volume-weighted average buy price
  buyRecords: number; // Số lượng record buy
  buyWallets: number; // Số ví buy
  buyVolume: number; // Tổng khối lượng buy
  // Sell statistics
  sellPrice: number; // Volume-weighted average sell price
  sellRecords: number; // Số lượng record sell
  sellWallets: number; // Số ví sell
  sellVolume: number; // Tổng khối lượng sell
  // Combined
  totalRecords: number;
  totalWallets: number;
  totalVolume: number;
}

/**
 * Track price changes over time
 * Groups trades by time intervals and calculates average/weighted average price
 */
export function trackPriceOverTime(
  orders: OrderMatched[],
  intervalSeconds: number = 60, // Default: 1 minute intervals
  outcomeIndex?: number
): PriceSnapshot[] {
  // Filter by outcome if specified
  const filteredOrders =
    outcomeIndex !== undefined
      ? orders.filter((o) => o.outcomeIndex === outcomeIndex)
      : orders;

  if (filteredOrders.length === 0) return [];

  // Sort by timestamp
  const sortedOrders = filteredOrders
    .filter((o) => o.onChainTimestamp !== undefined)
    .sort((a, b) => (a.onChainTimestamp || 0) - (b.onChainTimestamp || 0));

  if (sortedOrders.length === 0) return [];

  const firstTimestamp = sortedOrders[0].onChainTimestamp!;
  const lastTimestamp = sortedOrders[sortedOrders.length - 1].onChainTimestamp!;

  // Create time buckets
  const snapshots: PriceSnapshot[] = [];
  let currentBucketStart = firstTimestamp;

  while (currentBucketStart <= lastTimestamp) {
    const bucketEnd = currentBucketStart + intervalSeconds;
    const bucketOrders = sortedOrders.filter(
      (o) =>
        o.onChainTimestamp! >= currentBucketStart &&
        o.onChainTimestamp! < bucketEnd
    );

    if (bucketOrders.length > 0) {
      // Calculate volume-weighted average price
      let totalValue = 0;
      let totalVolume = 0;
      let totalTrades = 0;

      bucketOrders.forEach((order) => {
        const size = order.size || 0;
        const price = order.price || 0;
        totalValue += size * price;
        totalVolume += size;
        totalTrades += 1;
      });

      const avgPrice = totalVolume > 0 ? totalValue / totalVolume : 0;

      snapshots.push({
        timestamp: currentBucketStart,
        price: avgPrice,
        outcomeIndex,
        outcome: bucketOrders[0].outcome,
        volume: totalVolume,
        trades: totalTrades,
      });
    }

    currentBucketStart = bucketEnd;
  }

  return snapshots;
}

/**
 * Track price changes by timestamp with separate buy/sell statistics
 * Groups trades by exact timestamp and calculates statistics for each timestamp
 */
export function trackPriceByTimestamp(
  orders: OrderMatched[],
  outcomeIndex?: number
): DetailedPriceSnapshot[] {
  // Filter by outcome if specified
  const filteredOrders =
    outcomeIndex !== undefined
      ? orders.filter((o) => o.outcomeIndex === outcomeIndex)
      : orders;

  if (filteredOrders.length === 0) return [];

  // Filter orders with timestamp and sort by timestamp
  const sortedOrders = filteredOrders
    .filter((o) => o.onChainTimestamp !== undefined)
    .sort((a, b) => (a.onChainTimestamp || 0) - (b.onChainTimestamp || 0));

  if (sortedOrders.length === 0) return [];

  // Group by timestamp
  const timestampMap = new Map<number, OrderMatched[]>();
  sortedOrders.forEach((order) => {
    const timestamp = order.onChainTimestamp!;
    if (!timestampMap.has(timestamp)) {
      timestampMap.set(timestamp, []);
    }
    timestampMap.get(timestamp)!.push(order);
  });

  // Calculate statistics for each timestamp
  const snapshots: DetailedPriceSnapshot[] = [];
  const sortedTimestamps = Array.from(timestampMap.keys()).sort(
    (a, b) => a - b
  );

  sortedTimestamps.forEach((timestamp) => {
    const timestampOrders = timestampMap.get(timestamp)!;

    // Separate buy and sell orders
    const buyOrders = timestampOrders.filter((o) => o.side === "BUY");
    const sellOrders = timestampOrders.filter((o) => o.side === "SELL");

    // Calculate buy statistics
    let buyTotalValue = 0;
    let buyTotalVolume = 0;
    const buyWalletsSet = new Set<string>();
    buyOrders.forEach((order) => {
      const size = order.size || 0;
      const price = order.price || 0;
      buyTotalValue += size * price;
      buyTotalVolume += size;
      if (order.wallet) {
        buyWalletsSet.add(order.wallet.toLowerCase());
      }
    });
    const buyPrice = buyTotalVolume > 0 ? buyTotalValue / buyTotalVolume : 0;

    // Calculate sell statistics
    let sellTotalValue = 0;
    let sellTotalVolume = 0;
    const sellWalletsSet = new Set<string>();
    sellOrders.forEach((order) => {
      const size = order.size || 0;
      const price = order.price || 0;
      sellTotalValue += size * price;
      sellTotalVolume += size;
      if (order.wallet) {
        sellWalletsSet.add(order.wallet.toLowerCase());
      }
    });
    const sellPrice =
      sellTotalVolume > 0 ? sellTotalValue / sellTotalVolume : 0;

    // Calculate combined statistics
    const allWalletsSet = new Set<string>();
    timestampOrders.forEach((order) => {
      if (order.wallet) {
        allWalletsSet.add(order.wallet.toLowerCase());
      }
    });

    snapshots.push({
      timestamp,
      outcomeIndex,
      outcome: timestampOrders[0].outcome,
      buyPrice,
      buyRecords: buyOrders.length,
      buyWallets: buyWalletsSet.size,
      buyVolume: buyTotalVolume,
      sellPrice,
      sellRecords: sellOrders.length,
      sellWallets: sellWalletsSet.size,
      sellVolume: sellTotalVolume,
      totalRecords: timestampOrders.length,
      totalWallets: allWalletsSet.size,
      totalVolume: buyTotalVolume + sellTotalVolume,
    });
  });

  return snapshots;
}

/**
 * Simple CSV parser that handles quoted fields
 */
function parseCSVLine(line: string): string[] {
  const result: string[] = [];
  let current = "";
  let inQuotes = false;

  for (let i = 0; i < line.length; i++) {
    const char = line[i];
    const nextChar = line[i + 1];

    if (char === '"') {
      if (inQuotes && nextChar === '"') {
        // Escaped quote
        current += '"';
        i++; // Skip next quote
      } else {
        // Toggle quote state
        inQuotes = !inQuotes;
      }
    } else if (char === "," && !inQuotes) {
      // Field separator
      result.push(current);
      current = "";
    } else {
      current += char;
    }
  }

  // Add last field
  result.push(current);
  return result;
}

/**
 * Read all CSV files from a session directory and parse them
 */
export async function readSessionCSVFiles(
  sessionSlug: string
): Promise<OrderMatched[]> {
  const sessionDir = path.join(process.cwd(), "data", "sessions", sessionSlug);

  try {
    const files = await fs.readdir(sessionDir);
    const csvFiles = files.filter((f) => f.endsWith(".csv"));

    if (csvFiles.length === 0) {
      console.log(`No CSV files found in ${sessionDir}`);
      return [];
    }

    const allOrders: OrderMatched[] = [];

    for (const csvFile of csvFiles) {
      const filePath = path.join(sessionDir, csvFile);
      const content = await fs.readFile(filePath, "utf-8");
      const lines = content
        .trim()
        .split("\n")
        .filter((line) => line.trim() && !line.startsWith("receivedAt")); // Skip header

      for (const line of lines) {
        try {
          const parts = parseCSVLine(line);
          if (parts.length < 10) continue;

          // Parse CSV row: receivedAt,eventSlug,wallet,side,size,price,outcome,outcomeIndex,onChainTimestamp,transactionHash
          const order: OrderMatched = {
            receivedAt: parts[0]?.trim() || "",
            eventSlug: parts[1]?.trim() || undefined,
            wallet: parts[2]?.trim() || undefined,
            side: (parts[3]?.trim() as "BUY" | "SELL") || undefined,
            size: parseFloat(parts[4]?.trim() || "0") || undefined,
            price: parseFloat(parts[5]?.trim() || "0") || undefined,
            outcome: parts[6]?.trim() || undefined,
            outcomeIndex: parseInt(parts[7]?.trim() || "0", 10) || undefined,
            onChainTimestamp:
              parseInt(parts[8]?.trim() || "0", 10) || undefined,
            transactionHash: parts[9]?.trim() || undefined,
          };

          if (
            order.price !== undefined &&
            order.size !== undefined &&
            order.price > 0
          ) {
            allOrders.push(order);
          }
        } catch (err) {
          console.error(
            `Failed to parse CSV line: ${line.substring(0, 100)}...`,
            err
          );
        }
      }
    }

    console.log(
      `✅ Loaded ${allOrders.length} orders from ${csvFiles.length} CSV files`
    );
    return allOrders;
  } catch (err) {
    console.error(`Failed to read session directory: ${sessionDir}`, err);
    return [];
  }
}

/**
 * Example usage function
 */
async function main() {
  // Try to read from session CSV files first, fallback to JSONL
  const sessionSlug = process.argv[2] || "btc-updown-15m-1766141100";
  let orders: OrderMatched[] = [];

  // Try reading from session directory
  console.log(`📂 Reading orders from session: ${sessionSlug}`);
  orders = await readSessionCSVFiles(sessionSlug);

  // Fallback to JSONL if no CSV files found
  if (orders.length === 0) {
    const filePath = path.join(process.cwd(), "data", "orders_matched.jsonl");
    console.log("📊 Reading orders from:", filePath);
    try {
      orders = await readOrdersMatched(filePath);
    } catch (err) {
      console.error("❌ Failed to read orders:", err);
      return;
    }
  }

  console.log(`✅ Loaded ${orders.length} orders`);

  if (orders.length === 0) {
    console.log(
      "⚠️  No orders found. Make sure you have data in the session directory or JSONL file."
    );
    return;
  }

  // Example: Analyze by price
  console.log("\n📊 === PHÂN TÍCH THEO GIÁ ===");
  const priceAnalysis = analyzeByPrice(orders);
  const sortedPrices = Array.from(priceAnalysis.keys()).sort((a, b) => a - b);

  console.log(`\nTổng số mức giá khác nhau: ${sortedPrices.length}`);
  console.log("\nTop 10 mức giá có khối lượng cao nhất:");
  const topPrices = Array.from(priceAnalysis.values())
    .sort((a, b) => b.totalVolume - a.totalVolume)
    .slice(0, 10);

  topPrices.forEach((stats, idx) => {
    console.log(
      `${idx + 1}. Giá: ${stats.price.toFixed(2)} | ` +
        `Mua: ${stats.buyWallets} ví, ${stats.buyVolume.toFixed(2)} volume | ` +
        `Bán: ${stats.sellWallets} ví, ${stats.sellVolume.toFixed(
          2
        )} volume | ` +
        `Tổng: ${stats.totalVolume.toFixed(2)} volume, ${
          stats.totalTrades
        } trades`
    );
  });

  // Example: Get stats for a specific price (e.g., 50)
  const targetPrice = 50;
  console.log(`\n📈 === PHÂN TÍCH CHI TIẾT TẠI GIÁ ${targetPrice} ===`);
  const priceStats = getPriceStats(orders, targetPrice);
  if (priceStats) {
    console.log(`Giá: ${priceStats.price}`);
    console.log(`Số ví mua: ${priceStats.buyWallets}`);
    console.log(`Số ví bán: ${priceStats.sellWallets}`);
    console.log(`Khối lượng mua: ${priceStats.buyVolume.toFixed(2)}`);
    console.log(`Khối lượng bán: ${priceStats.sellVolume.toFixed(2)}`);
    console.log(`Số giao dịch mua: ${priceStats.buyTrades}`);
    console.log(`Số giao dịch bán: ${priceStats.sellTrades}`);
    console.log(`Tổng khối lượng: ${priceStats.totalVolume.toFixed(2)}`);
    console.log(`Tổng số giao dịch: ${priceStats.totalTrades}`);
  } else {
    console.log(`Không tìm thấy giao dịch tại giá ${targetPrice}`);
    // Show closest prices
    const closestPrices = sortedPrices
      .map((p) => ({ price: p, diff: Math.abs(p - targetPrice) }))
      .sort((a, b) => a.diff - b.diff)
      .slice(0, 5);
    console.log("\nCác mức giá gần nhất:");
    closestPrices.forEach((p) => {
      const stats = priceAnalysis.get(p.price)!;
      console.log(
        `  Giá: ${p.price.toFixed(2)} (chênh lệch: ${p.diff.toFixed(2)}) | ` +
          `Mua: ${stats.buyWallets} ví, Bán: ${stats.sellWallets} ví`
      );
    });
  }

  // Example: Track price over time
  console.log("\n📈 === THEO DÕI GIÁ THEO THỜI GIAN ===");
  const priceHistory = trackPriceOverTime(orders, 60); // 1 minute intervals
  if (priceHistory.length > 0) {
    console.log(`Đã ghi nhận ${priceHistory.length} mốc thời gian`);
    console.log("\n10 mốc thời gian đầu tiên:");
    priceHistory.slice(0, 10).forEach((snapshot) => {
      const date = new Date(snapshot.timestamp * 1000).toISOString();
      console.log(
        `${date} | Giá: ${snapshot.price.toFixed(2)} | ` +
          `Volume: ${snapshot.volume.toFixed(2)} | Trades: ${snapshot.trades}`
      );
    });

    if (priceHistory.length > 10) {
      console.log("\n10 mốc thời gian cuối cùng:");
      priceHistory.slice(-10).forEach((snapshot) => {
        const date = new Date(snapshot.timestamp * 1000).toISOString();
        console.log(
          `${date} | Giá: ${snapshot.price.toFixed(2)} | ` +
            `Volume: ${snapshot.volume.toFixed(2)} | Trades: ${snapshot.trades}`
        );
      });
    }

    // Calculate price change
    if (priceHistory.length > 1) {
      const firstPrice = priceHistory[0].price;
      const lastPrice = priceHistory[priceHistory.length - 1].price;
      const priceChange = lastPrice - firstPrice;
      const priceChangePercent =
        firstPrice > 0 ? (priceChange / firstPrice) * 100 : 0;
      console.log(
        `\nBiến động giá: ${firstPrice.toFixed(2)} → ${lastPrice.toFixed(2)} ` +
          `(${priceChange >= 0 ? "+" : ""}${priceChange.toFixed(2)}, ` +
          `${priceChangePercent >= 0 ? "+" : ""}${priceChangePercent.toFixed(
            2
          )}%)`
      );
    }
  } else {
    console.log("Không có dữ liệu giá theo thời gian (thiếu onChainTimestamp)");
  }

  // Example: Get stats for a specific wallet
  const exampleWallet = process.argv[3] || getAllWallets(orders)[0];
  if (exampleWallet) {
    console.log(`\n📈 === THỐNG KÊ VÍ: ${exampleWallet} ===`);
    const walletStats = getWalletStats(orders, exampleWallet);
    console.log(JSON.stringify(walletStats, null, 2));
  }

  // Example: Get top wallets by trade count
  console.log("\n🏆 === TOP 10 VÍ THEO SỐ GIAO DỊCH ===");
  const topByCount = getTopWalletsByTradeCount(orders, 10);
  topByCount.forEach((stats, idx) => {
    console.log(
      `${idx + 1}. ${stats.wallet}: ${stats.totalTrades} trades (BUY: ${
        stats.buyCount
      }, SELL: ${stats.sellCount})`
    );
  });

  // Example: Get top wallets by volume
  console.log("\n💰 === TOP 10 VÍ THEO KHỐI LƯỢNG ===");
  const topByVolume = getTopWalletsByVolume(orders, 10);
  topByVolume.forEach((stats, idx) => {
    console.log(
      `${idx + 1}. ${stats.wallet}: ${stats.totalVolume.toFixed(
        2
      )} volume (BUY: ${stats.totalBuyVolume.toFixed(
        2
      )}, SELL: ${stats.totalSellVolume.toFixed(2)})`
    );
  });

  // Example: Get top wallets by profit
  console.log("\n💵 === TOP 10 VÍ THEO LỢI NHUẬN ===");
  const topByProfit = getTopWalletsByProfit(orders, 10);
  topByProfit.forEach((profit) => {
    console.log(
      `${profit.rank}. ${profit.wallet}: ${profit.realizedProfit.toFixed(
        4
      )} profit (Cost: ${profit.totalCost.toFixed(
        4
      )}, Revenue: ${profit.totalRevenue.toFixed(4)})`
    );
  });
}

// Run if executed directly
if (require.main === module) {
  main().catch(console.error);
}
