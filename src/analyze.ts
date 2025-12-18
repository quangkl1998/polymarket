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
 * Example usage function
 */
async function main() {
  const filePath = path.join(process.cwd(), "data", "orders_matched.jsonl");

  console.log("📊 Reading orders from:", filePath);
  const orders = await readOrdersMatched(filePath);
  console.log(`✅ Loaded ${orders.length} orders`);

  // Example: Get stats for a specific wallet
  const exampleWallet = "0x8Ac1EAed0399f8332f47041436312d6Cd4b19595";
  const walletStats = getWalletStats(orders, exampleWallet);
  console.log("\n📈 Stats for wallet:", exampleWallet);
  console.log(JSON.stringify(walletStats, null, 2));

  // Example: Calculate profit for a specific wallet
  console.log("\n💰 Profit for wallet:", exampleWallet);
  const walletProfit = calculateWalletProfit(orders, exampleWallet);
  console.log(`  Total Cost: ${walletProfit.totalCost.toFixed(4)}`);
  console.log(`  Total Revenue: ${walletProfit.totalRevenue.toFixed(4)}`);
  console.log(`  Realized Profit: ${walletProfit.realizedProfit.toFixed(4)}`);
  console.log(
    `  Unrealized Profit: ${walletProfit.unrealizedProfit.toFixed(4)}`
  );
  console.log(`  Total Profit: ${walletProfit.totalProfit.toFixed(4)}`);
  console.log(`  Open Position: ${walletProfit.openPosition.toFixed(4)}`);

  if (walletProfit.profitByOutcome.size > 0) {
    console.log("\n  Profit by Outcome:");
    walletProfit.profitByOutcome.forEach((outcomeProfit) => {
      console.log(
        `    Outcome ${outcomeProfit.outcomeIndex} (${
          outcomeProfit.outcome || "N/A"
        }):`
      );
      console.log(
        `      Realized: ${outcomeProfit.realizedProfit.toFixed(
          4
        )}, Position: ${outcomeProfit.openPosition.toFixed(4)}`
      );
      console.log(
        `      Avg Buy: ${outcomeProfit.averageBuyPrice.toFixed(
          4
        )}, Avg Sell: ${outcomeProfit.averageSellPrice.toFixed(4)}`
      );
    });
  }

  // Example: Get top wallets by trade count
  console.log("\n🏆 Top 10 wallets by trade count:");
  const topByCount = getTopWalletsByTradeCount(orders, 10);
  topByCount.forEach((stats, idx) => {
    console.log(
      `${idx + 1}. ${stats.wallet}: ${stats.totalTrades} trades (BUY: ${
        stats.buyCount
      }, SELL: ${stats.sellCount})`
    );
  });

  // Example: Get top wallets by volume
  console.log("\n💰 Top 10 wallets by volume:");
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
  console.log("\n💵 Top 10 wallets by realized profit:");
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
