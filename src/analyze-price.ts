#!/usr/bin/env ts-node

import {
  readSessionCSVFiles,
  analyzeByPrice,
  getPriceStats,
  trackPriceOverTime,
  trackPriceByTimestamp,
} from "./analyze";
import path from "path";
import { promises as fs } from "fs";

/**
 * Analyze price-specific data from a session
 * Usage: ts-node src/analyze-price.ts <session-slug> <price> [outcome-index]
 */
async function main() {
  const sessionSlug = process.argv[2];
  const priceArg = process.argv[3];
  const outcomeIndexArg = process.argv[4];

  if (!sessionSlug) {
    console.error(
      "❌ Usage: ts-node src/analyze-price.ts <session-slug> <price> [outcome-index]"
    );
    console.error(
      "   Example: ts-node src/analyze-price.ts btc-updown-15m-1766141100 50"
    );
    process.exit(1);
  }

  console.log(`📂 Đang đọc dữ liệu từ session: ${sessionSlug}`);
  const orders = await readSessionCSVFiles(sessionSlug);

  if (orders.length === 0) {
    console.error("❌ Không tìm thấy dữ liệu trong session này");
    process.exit(1);
  }

  console.log(`✅ Đã tải ${orders.length} giao dịch\n`);

  // If price is specified, analyze that specific price
  if (priceArg) {
    const targetPrice = parseFloat(priceArg);
    const outcomeIndex = outcomeIndexArg
      ? parseInt(outcomeIndexArg, 10)
      : undefined;

    if (isNaN(targetPrice)) {
      console.error(`❌ Giá không hợp lệ: ${priceArg}`);
      process.exit(1);
    }

    console.log(`📊 === PHÂN TÍCH CHI TIẾT TẠI GIÁ ${targetPrice} ===`);
    if (outcomeIndex !== undefined) {
      console.log(`   (Lọc theo outcome index: ${outcomeIndex})`);
    }
    console.log();

    const priceStats = getPriceStats(orders, targetPrice, outcomeIndex);

    if (priceStats) {
      console.log(`💰 Giá: ${priceStats.price}`);
      console.log(`📈 Số ví mua: ${priceStats.buyWallets}`);
      console.log(`📉 Số ví bán: ${priceStats.sellWallets}`);
      console.log(`📊 Khối lượng mua: ${priceStats.buyVolume.toFixed(4)}`);
      console.log(`📊 Khối lượng bán: ${priceStats.sellVolume.toFixed(4)}`);
      console.log(`🔄 Số giao dịch mua: ${priceStats.buyTrades}`);
      console.log(`🔄 Số giao dịch bán: ${priceStats.sellTrades}`);
      console.log(`📦 Tổng khối lượng: ${priceStats.totalVolume.toFixed(4)}`);
      console.log(`📝 Tổng số giao dịch: ${priceStats.totalTrades}`);
      console.log(`👥 Tổng số ví tham gia: ${priceStats.wallets.size}`);

      // Show some wallet examples
      if (priceStats.wallets.size > 0) {
        const walletArray = Array.from(priceStats.wallets);
        console.log(`\n📋 Ví dụ một số ví tham gia (tối đa 10):`);
        walletArray.slice(0, 10).forEach((wallet, idx) => {
          console.log(`   ${idx + 1}. ${wallet}`);
        });
        if (walletArray.length > 10) {
          console.log(`   ... và ${walletArray.length - 10} ví khác`);
        }
      }
    } else {
      console.log(`❌ Không tìm thấy giao dịch tại giá ${targetPrice}`);

      // Show closest prices
      const priceAnalysis = analyzeByPrice(orders, outcomeIndex);
      const sortedPrices = Array.from(priceAnalysis.keys()).sort(
        (a, b) => a - b
      );

      if (sortedPrices.length > 0) {
        const closestPrices = sortedPrices
          .map((p) => ({ price: p, diff: Math.abs(p - targetPrice) }))
          .sort((a, b) => a.diff - b.diff)
          .slice(0, 5);

        console.log("\n💡 Các mức giá gần nhất:");
        closestPrices.forEach((p) => {
          const stats = priceAnalysis.get(p.price)!;
          console.log(
            `   Giá: ${p.price.toFixed(2)} (chênh lệch: ${p.diff.toFixed(
              2
            )}) | ` +
              `Mua: ${stats.buyWallets} ví, Bán: ${stats.sellWallets} ví | ` +
              `Volume: ${stats.totalVolume.toFixed(2)}`
          );
        });
      }
    }
  }

  // Track price history by timestamp
  const outcomeIndex = outcomeIndexArg
    ? parseInt(outcomeIndexArg, 10)
    : undefined;
  const priceHistory = trackPriceByTimestamp(orders, outcomeIndex);

  if (priceHistory.length > 0) {
    console.log(
      `\n📈 Đã ghi nhận ${priceHistory.length} mốc thời gian (mỗi khi giá thay đổi)`
    );

    // Log all price history
    priceHistory.forEach((snapshot) => {
      const buyInfo =
        snapshot.buyPrice > 0
          ? `giá buy ${snapshot.buyPrice.toFixed(2)}, số lượng ${
              snapshot.buyRecords
            } record, ${
              snapshot.buyWallets
            } ví, tổng khối lượng ${snapshot.buyVolume.toFixed(2)}`
          : "không có giao dịch buy";
      const sellInfo =
        snapshot.sellPrice > 0
          ? `giá sell ${snapshot.sellPrice.toFixed(2)}, số lượng ${
              snapshot.sellRecords
            } record, ${
              snapshot.sellWallets
            } ví, tổng khối lượng ${snapshot.sellVolume.toFixed(2)}`
          : "không có giao dịch sell";

      console.log(`time ${snapshot.timestamp}, ${buyInfo}, ${sellInfo}`);
    });

    // Save to CSV file
    const csvDir = path.join(process.cwd(), "data", "sessions", sessionSlug);
    const csvFileName =
      outcomeIndex !== undefined
        ? `price-history-outcome-${outcomeIndex}.csv`
        : "price-history.csv";
    const csvFilePath = path.join(csvDir, csvFileName);

    // CSV header
    const csvHeader =
      "timestamp,datetime,buyPrice,buyRecords,buyWallets,buyVolume,sellPrice,sellRecords,sellWallets,sellVolume,totalRecords,totalWallets,totalVolume,outcomeIndex,outcome";

    // CSV rows
    const csvRows = priceHistory.map((snapshot) => {
      const datetime = new Date(snapshot.timestamp * 1000).toISOString();
      return [
        snapshot.timestamp,
        datetime,
        snapshot.buyPrice > 0 ? snapshot.buyPrice.toFixed(4) : "",
        snapshot.buyRecords,
        snapshot.buyWallets,
        snapshot.buyVolume.toFixed(4),
        snapshot.sellPrice > 0 ? snapshot.sellPrice.toFixed(4) : "",
        snapshot.sellRecords,
        snapshot.sellWallets,
        snapshot.sellVolume.toFixed(4),
        snapshot.totalRecords,
        snapshot.totalWallets,
        snapshot.totalVolume.toFixed(4),
        snapshot.outcomeIndex !== undefined ? snapshot.outcomeIndex : "",
        snapshot.outcome || "",
      ].join(",");
    });

    // Write CSV file
    try {
      await fs.mkdir(csvDir, { recursive: true });
      const csvContent = csvHeader + "\n" + csvRows.join("\n");
      await fs.writeFile(csvFilePath, csvContent, "utf-8");
      console.log(`\n✅ Đã lưu lịch sử giá vào: ${csvFilePath}`);
    } catch (err) {
      console.error(`\n❌ Lỗi khi lưu CSV: ${err}`);
    }

    // Calculate price change
    if (priceHistory.length > 1) {
      const firstBuyPrice = priceHistory[0].buyPrice;
      const lastBuyPrice = priceHistory[priceHistory.length - 1].buyPrice;
      const firstSellPrice = priceHistory[0].sellPrice;
      const lastSellPrice = priceHistory[priceHistory.length - 1].sellPrice;

      console.log(`\n📊 Biến động giá:`);
      if (firstBuyPrice > 0 && lastBuyPrice > 0) {
        const buyPriceChange = lastBuyPrice - firstBuyPrice;
        const buyPriceChangePercent =
          firstBuyPrice > 0 ? (buyPriceChange / firstBuyPrice) * 100 : 0;
        console.log(
          `   Giá Buy: ${firstBuyPrice.toFixed(4)} → ${lastBuyPrice.toFixed(
            4
          )} ` +
            `(${buyPriceChange >= 0 ? "+" : ""}${buyPriceChange.toFixed(4)}, ` +
            `${
              buyPriceChangePercent >= 0 ? "+" : ""
            }${buyPriceChangePercent.toFixed(2)}%)`
        );
      }
      if (firstSellPrice > 0 && lastSellPrice > 0) {
        const sellPriceChange = lastSellPrice - firstSellPrice;
        const sellPriceChangePercent =
          firstSellPrice > 0 ? (sellPriceChange / firstSellPrice) * 100 : 0;
        console.log(
          `   Giá Sell: ${firstSellPrice.toFixed(4)} → ${lastSellPrice.toFixed(
            4
          )} ` +
            `(${sellPriceChange >= 0 ? "+" : ""}${sellPriceChange.toFixed(
              4
            )}, ` +
            `${
              sellPriceChangePercent >= 0 ? "+" : ""
            }${sellPriceChangePercent.toFixed(2)}%)`
        );
      }
    }
  } else {
    console.log(
      "⚠️  Không có dữ liệu giá theo thời gian (thiếu onChainTimestamp)"
    );
  }
}

main().catch((err) => {
  console.error("❌ Lỗi:", err);
  process.exit(1);
});
