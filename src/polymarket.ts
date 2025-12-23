import axios from "axios";

const API_BASE = "https://clob.polymarket.com";
const DATA_API_BASE = "https://data-api.polymarket.com";

/**
 * Get condition_id from market slug
 */
export async function getConditionIdBySlug(slug: string): Promise<string> {
  const marketRes = await axios.get(`${API_BASE}/markets`, {
    params: { slug },
  });

  const markets = marketRes.data?.data ?? marketRes.data;
  if (!markets || markets.length === 0) {
    throw new Error("Market not found");
  }

  const conditionId = markets[0].condition_id;
  if (!conditionId) {
    throw new Error("condition_id not found for this slug");
  }
  return conditionId;
}

/**
 * Get asset_ids (CLOB token IDs) from market slug using Gamma API
 * Returns array with first clobTokenId: [clobTokenIds[0]]
 */
export async function getAssetIdsBySlug(slug: string): Promise<string[]> {
  try {
    // Use Gamma API to get market data
    const marketRes = await axios.get(
      `https://gamma-api.polymarket.com/markets/slug/${slug}`
    );

    const market = marketRes.data;
    if (!market) {
      throw new Error("Market not found in response");
    }

    // Parse clobTokenIds from JSON string
    let clobTokenIds: string[] = [];
    if (market.clobTokenIds) {
      if (typeof market.clobTokenIds === "string") {
        // Parse JSON string
        try {
          clobTokenIds = JSON.parse(market.clobTokenIds);
        } catch (err) {
          console.warn("Failed to parse clobTokenIds:", err);
        }
      } else if (Array.isArray(market.clobTokenIds)) {
        clobTokenIds = market.clobTokenIds;
      }
    }

    if (clobTokenIds.length === 0) {
      throw new Error("No clobTokenIds found in market response");
    }

    // Return array with first token ID: [clobTokenIds[0]]
    return [clobTokenIds[0]];
  } catch (err: any) {
    const errorMsg =
      err?.response?.status === 404
        ? "Market not found (404)"
        : err?.message || "Unknown error";
    throw new Error(`Failed to get asset IDs for slug ${slug}: ${errorMsg}`);
  }
}

type TradeSide = "BUY" | "SELL";

export interface TradesQuery {
  conditionId?: string;
  wallet?: string;
  side?: TradeSide;
  limit?: number;
  offset?: number;
}

export interface TradeItem {
  size: number;
  price: number;
  side: TradeSide;
  proxyWallet?: string;
  transactionHash?: string;
  outcome?: string;
  outcomeIndex?: number;
  timestamp?: number;
  conditionId?: string;
}

/**
 * Fetch historical trades (orders_matched) from Polymarket REST data API.
 * At least one of conditionId or wallet should be provided to limit results.
 */
export async function fetchTrades({
  conditionId,
  wallet,
  side,
  limit = 100,
  offset = 0,
}: TradesQuery): Promise<TradeItem[]> {
  if (!conditionId && !wallet) {
    throw new Error("Please provide conditionId or wallet to fetch trades");
  }

  const params: Record<string, string | number> = {
    limit,
    offset,
  };

  if (conditionId) params.market = conditionId;
  if (wallet) params.user = wallet;
  if (side) params.side = side;

  const res = await axios.get(`${DATA_API_BASE}/trades`, { params });
  return res.data as TradeItem[];
}
