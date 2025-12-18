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
 * Get asset_ids (YES / NO) from market slug
 */
export async function getAssetIdsBySlug(slug: string): Promise<string[]> {
  const conditionId = await getConditionIdBySlug(slug);

  // 2. Get tokens (YES / NO)
  const tokensRes = await axios.get(`${API_BASE}/tokens`, {
    params: {
      condition_id: conditionId,
    },
  });

  if (!tokensRes.data || tokensRes.data.length === 0) {
    throw new Error("No tokens found");
  }

  // Extract asset_id
  const assetIds = tokensRes.data.map((t: any) => t.asset_id);

  return assetIds;
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
