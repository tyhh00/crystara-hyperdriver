import postgres from "postgres";

export interface Env {
  // If you set another name in wrangler.toml as the value for 'binding',
  // replace "HYPERDRIVE" with the variable name you defined.
  HYPERDRIVE: Hyperdrive;
  VALID_API_KEYS: string;  // Will contain the JSON string
}

// Add CORS headers to all responses
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Max-Age': '86400',
};

async function validateApiKey(request: Request, env: Env): Promise<boolean> {
  const apiKey = request.headers.get('x-api-key');
  const validKeys = JSON.parse(env.VALID_API_KEYS);
  return apiKey !== null && validKeys.includes(apiKey);
}

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    // Handle CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders });
    }

    const url = new URL(request.url);
    const path = url.pathname;

    // Check if route requires API key
    if (path.startsWith('/api/private/')) {
      if (!await validateApiKey(request, env)) {
        return Response.json(
          { error: 'Unauthorized - Invalid API key' },
          { status: 401, headers: corsHeaders }
        );
      }
    }

    const sql = postgres(env.HYPERDRIVE.connectionString);

    try {
      const response = await handleRoute(path, sql, url, request);
      Object.entries(corsHeaders).forEach(([key, value]) => {
        response.headers.set(key, value);
      });
      return response;
    } catch (error: any) {
      console.error('Error:', error);
      return Response.json(
        { error: error.message },
        { status: 500, headers: corsHeaders }
      );
    } finally {
      await sql.end();
    }
  },
} satisfies ExportedHandler<Env>;

// Helper function to handle routes
async function handleRoute(path: string, sql: postgres.Sql, url: URL, request: Request): Promise<Response> {
  switch (path) {
    // Account routes
    case '/api/accounts': {
      return await getAccounts(sql);
    }
    case '/api/account-balances': {
      const address = url.searchParams.get('address');
      if (!address) return Response.json({ error: 'Address required' }, { status: 400 });
      return await getAccountBalances(sql, address);
    }

    // Lootbox routes
    case '/api/lootboxes': {
      return await getLootboxes(sql);
    }
    case '/api/lootbox-analytics': {
      return await getLootboxAnalytics(sql);
    }
    case '/api/lootbox-purchases': {
      return await getLootboxPurchases(sql);
    }
    case '/api/lootbox-rewards': {
      return await getLootboxRewards(sql);
    }
    case '/api/lootbox': {
      const creator = url.searchParams.get('creator');
      const collection = url.searchParams.get('collection');
      if (!creator || !collection) {
        return Response.json({ error: 'Creator address and collection name required' }, { status: 400 });
      }
      return await getLootboxByCreatorAndCollection(sql, creator, collection);
    }

    // Token routes
    case '/api/tokens': {
      return await getTokens(sql);
    }
    case '/api/token-collections': {
      return await getTokenCollections(sql);
    }
    case '/api/token-transactions': {
      return await getTokenTransactions(sql);
    }
    case '/api/token-balances': {
      const address = url.searchParams.get('address');
      const greaterThanZeroBalance = url.searchParams.get('greaterThanZeroBalance') === 'true';

      return await getTokenBalances(sql, {
        address,
        greaterThanZeroBalance
      });
    }
    case '/api/token-data': {
      return await getTokenData(sql);
    }
    case '/api/token-deposits': {
      return await getTokenDeposits(sql);
    }
    case '/api/token-withdraws': {
      return await getTokenWithdraws(sql);
    }
    case '/api/token-burns': {
      return await getTokenBurns(sql);
    }
    case '/api/token-mints': {
      return await getTokenMints(sql);
    }
    case '/api/token-claims': {
      return await getTokenClaims(sql);
    }
    case '/api/tokens-by-lootbox': {
      const tokenLootboxId = url.searchParams.get('lootboxId');
      if (!tokenLootboxId) return Response.json({ error: 'Lootbox ID required' }, { status: 400 });
      return await getTokensByLootboxId(sql, parseInt(tokenLootboxId));
    }
    case '/api/tokens-by-collection': {
      const creator = url.searchParams.get('creator');
      const collection = url.searchParams.get('collection');
      if (!creator || !collection) {
        return Response.json({ error: 'Creator address and collection name required' }, { status: 400 });
      }
      return await getTokensByCreatorAndCollection(sql, creator, collection);
    }
    case '/api/tokens-by-lootbox-creator': {
      const creatorAddress = url.searchParams.get('creator');
      const collectionName = url.searchParams.get('collection');
      if (!creatorAddress || !collectionName) {
        return Response.json({ error: 'Creator address and collection name required' }, { status: 400 });
      }
      return await getTokensByLootboxCreator(sql, creatorAddress, collectionName);
    }
    case '/api/token-collection-by-lootbox-creator': {
      const creatorAddress = url.searchParams.get('creator');
      const collectionName = url.searchParams.get('collection');
      if (!creatorAddress || !collectionName) {
        return Response.json({ error: 'Creator address and collection name required' }, { status: 400 });
      }
      return await getTokenCollectionByLootbox(sql, creatorAddress, collectionName);
    }

    // Rarity routes
    case '/api/rarities': {
      return await getRarities(sql);
    }
    case '/api/rarities-by-lootbox': {
      const lootboxId = url.searchParams.get('lootboxId');
      if (!lootboxId) return Response.json({ error: 'Lootbox ID required' }, { status: 400 });
      return await getRaritiesByLootboxId(sql, parseInt(lootboxId));
    }
    case '/api/rarities-by-collection': {
      const creator = url.searchParams.get('creator');
      const collection = url.searchParams.get('collection');
      if (!creator || !collection) {
        return Response.json({ error: 'Creator address and collection name required' }, { status: 400 });
      }
      return await getRaritiesByCreatorAndCollection(sql, creator, collection);
    }

    // Event routes
    case '/api/event-tracking': {
      return await getEventTracking(sql);
    }

    // VRF routes
    case '/api/vrf-callbacks': {
      return await getVRFCallbacks(sql);
    }

    // Private routes
    case '/api/private/accounts': {
      return await getOffChainAccounts(sql);
    }
    case '/api/private/account': {
      const walletAddress = url.searchParams.get('address');
      if (!walletAddress) return Response.json({ error: 'Wallet address required' }, { status: 400 });
      return await getOffChainAccount(sql, walletAddress);
    }
    case '/api/private/lootbox-stats': {
      const lootboxId = url.searchParams.get('lootboxId');
      if (!lootboxId) return Response.json({ error: 'Lootbox ID required' }, { status: 400 });
      return await getLootboxStats(sql, parseInt(lootboxId));
    }
    case '/api/private/lootbox-likes': {
      const lootboxId = url.searchParams.get('lootboxId');
      if (!lootboxId) return Response.json({ error: 'Lootbox ID required' }, { status: 400 });
      return await getLootboxLikes(sql, parseInt(lootboxId));
    }
    case '/api/private/lootbox-views': {
      const lootboxId = url.searchParams.get('lootboxId');
      if (!lootboxId) return Response.json({ error: 'Lootbox ID required' }, { status: 400 });
      return await getLootboxViews(sql, parseInt(lootboxId));
    }
    case '/api/private/creator-lootbox-stats': {
      const creatorAddress = url.searchParams.get('creator');
      const includeLootboxes = url.searchParams.get('includeLootboxes') === 'true';
      const includeTokens = url.searchParams.get('includeTokens') === 'true';
      if (!creatorAddress) return Response.json({ error: 'Creator address required' }, { status: 400 });
      return await getCreatorLootboxStats(sql, creatorAddress, includeLootboxes, includeTokens);
    }
    case '/api/private/lootbox-stats-by-url': {
      const urlParam = url.searchParams.get('url');
      const includeLootbox = url.searchParams.get('includeLootbox') === 'true';
      const includeTokens = url.searchParams.get('includeTokens') === 'true';
      if (!urlParam) return Response.json({ error: 'URL required' }, { status: 400 });
      return await getLootboxStatsByUrl(sql, urlParam, includeLootbox, includeTokens);
    }
    case '/api/private/lootbox-stats-url-exists': {
      const urlParam = url.searchParams.get('url');
      if (!urlParam) return Response.json({ error: 'URL required' }, { status: 400 });
      return await checkLootboxStatsUrlExists(sql, urlParam);
    }
    case '/api/private/account/upsert': {
      const walletAddress = url.searchParams.get('walletAddress')?.trim();
      const email = url.searchParams.get('email')?.trim().toLowerCase();
      const username = url.searchParams.get('username')?.trim();
      const preferences = url.searchParams.get('preferences');
      
      // Input validation
      if (!walletAddress) {
        return Response.json({ error: 'Invalid wallet address format' }, { status: 400 });
      }

      // Email validation if provided
      if (email) {
        return Response.json({ error: 'Invalid email format' }, { status: 400 });
      }

      // Username validation if provided
      if (username) {
        if (username.length < 3 || username.length > 30) {
          return Response.json({ error: 'Username must be between 3 and 30 characters' }, { status: 400 });
        }
        if (!/^[a-zA-Z0-9_-]+$/.test(username)) {
          return Response.json({ error: 'Username can only contain letters, numbers, underscores, and hyphens' }, { status: 400 });
        }
      }

      // Preferences validation
      let parsedPreferences = null;
      if (preferences) {
        try {
          parsedPreferences = JSON.parse(preferences);
          if (typeof parsedPreferences !== 'object' || parsedPreferences === null) {
            throw new Error('Preferences must be an object');
          }
        } catch (e) {
          return Response.json({ error: 'Invalid preferences format' }, { status: 400 });
        }
      }

      // Rate limiting check (implement your rate limiting logic here)
      // const isRateLimited = await checkRateLimit(request.ip);
      // if (isRateLimited) {
      //   return Response.json({ error: 'Too many requests' }, { status: 429 });
      // }

      return await upsertOffChainAccount(sql, {
        walletAddress,
        email: email || null,
        username: username || null,
        preferences: parsedPreferences,
        lastLoginAt: new Date()
      });
    }
    case '/api/private/lootbox-stats/create': {
 
        const body = await request.json() as {
          lootboxId: number;
          url: string;
          rarityColors: Record<string, string>;
          creatorAddress: string;
          collectionName: string;
        };
        const { lootboxId, url, rarityColors, creatorAddress, collectionName } = body;

        if (!lootboxId || !url || !creatorAddress || !collectionName) {
          return Response.json({ error: 'Missing required fields' }, { status: 400 });
        }

        return await createOffchainLootboxStats(sql, {
          lootboxId,
          url,
          rarityColors,
          creatorAddress,
          collectionName
        });
      
    }
    case '/api/private/lootbox-stats-by-collection': {
      const creatorAddress = url.searchParams.get('creator');
      const collectionName = url.searchParams.get('collection');
      const includeLootbox = url.searchParams.get('includeLootbox') === 'true';
      const includeTokens = url.searchParams.get('includeTokens') === 'true';

      if (!creatorAddress || !collectionName) {
        return Response.json({ error: 'Creator address and collection name required' }, { status: 400 });
      }

      return await getLootboxStatsByCollection(sql, creatorAddress, collectionName, includeLootbox, includeTokens);
    }
    case '/api/private/all-lootbox-stats': {
      const mustHaveUrl = url.searchParams.get('mustHaveUrl') === 'true';
      const isActive = url.searchParams.get('isActive') === 'true';
      const isWhitelisted = url.searchParams.get('isWhitelisted') === 'true';
      const includeTokenCollection = url.searchParams.get('includeTokenCollection') === 'true';
      const includeTokens = url.searchParams.get('includeTokens') === 'true';

      return await getAllLootboxStats(sql, {
        mustHaveUrl,
        isActive,
        isWhitelisted,
        includeTokenCollection,
        includeTokens
      });
    }

    default: {
      return new Response(
        JSON.stringify({ error: 'Not found' }), 
        { 
          status: 404,
          headers: {
            'Content-Type': 'application/json'
          }
        }
      );
    }
  }
}

// Account queries
async function getAccounts(sql: postgres.Sql) {
  const accounts = await sql`
    SELECT * FROM "Account"
    ORDER BY "createdAt" DESC
    LIMIT 20
  `;
  return Response.json({ accounts });
}

async function getAccountBalances(sql: postgres.Sql, address: string) {
  const balances = await sql`
    SELECT tb.*, t."tokenName"
    FROM "TokenBalance" tb
    LEFT JOIN "Token" t ON t.id = tb."tokenId"
    WHERE tb."accountAddress" = ${address}
    ORDER BY tb."lastUpdated" DESC
  `;
  return Response.json({ balances });
}

// Lootbox queries
async function getLootboxes(sql: postgres.Sql) {
	const lootboxes = await sql`
	  SELECT 
		l.id,
		l."creatorAddress",
		l."collectionResourceAddress",
		l."collectionName",
		l."collectionDescription",
		l."metadataUri",
		l.price,
		l."priceCoinType",
		l."maxStock",
		l."availableStock",
		l."isActive",
		l."isWhitelisted",
		l."autoTriggerWhitelistTime",
		l."autoTriggerActiveTime",
		l."totalVolume",
		l."purchaseCount",
		l."createdAt",
		l."updatedAt",
		l."timestamp",
		la.id as "analyticsId",
		la."volume24h",
		la."purchases24h",
		la."uniqueBuyers24h",
		la."updatedAt" as "analyticsUpdatedAt"
	  FROM "Lootbox" l
	  LEFT JOIN "LootboxAnalytics" la ON la."lootboxId" = l.id
	  ORDER BY l."createdAt" DESC
	  LIMIT 20
	`;
	return Response.json({ lootboxes });
  }

async function getLootboxAnalytics(sql: postgres.Sql) {
  const analytics = await sql`
    SELECT * FROM "LootboxAnalytics"
    ORDER BY "updatedAt" DESC
    LIMIT 20
  `;
  return Response.json({ analytics });
}

// Token queries
async function getTokens(sql: postgres.Sql) {
  const tokens = await sql`
    SELECT t.*, tc."name" as "collectionName"
    FROM "Token" t
    LEFT JOIN "TokenCollection" tc ON tc.id = t."tokenCollectionId"
    ORDER BY t."createdAt" DESC
    LIMIT 20
  `;
  return Response.json({ tokens });
}

async function getTokenCollections(sql: postgres.Sql) {
  const collections = await sql`
    SELECT * FROM "TokenCollection"
    ORDER BY "createdAt" DESC
    LIMIT 20
  `;
  return Response.json({ collections });
}

async function getTokenData(sql: postgres.Sql) {
  const tokenData = await sql`
    SELECT * FROM "TokenData"
    ORDER BY "createdAt" DESC
    LIMIT 20
  `;
  return Response.json({ tokenData });
}

async function getTokenTransactions(sql: postgres.Sql) {
  const transactions = await sql`
    SELECT * FROM "TokenTransaction"
    ORDER BY "createdAt" DESC
    LIMIT 50
  `;
  return Response.json({ transactions });
}

async function getTokenDeposits(sql: postgres.Sql) {
  const deposits = await sql`
    SELECT * FROM "TokenDeposit"
    ORDER BY "createdAt" DESC
    LIMIT 20
  `;
  return Response.json({ deposits });
}

async function getTokenWithdraws(sql: postgres.Sql) {
  const withdraws = await sql`
    SELECT * FROM "TokenWithdraw"
    ORDER BY "createdAt" DESC
    LIMIT 20
  `;
  return Response.json({ withdraws });
}

async function getTokenBurns(sql: postgres.Sql) {
  const burns = await sql`
    SELECT * FROM "TokenBurn"
    ORDER BY "createdAt" DESC
    LIMIT 20
  `;
  return Response.json({ burns });
}

async function getTokenMints(sql: postgres.Sql) {
  const mints = await sql`
    SELECT * FROM "TokenMint"
    ORDER BY "createdAt" DESC
    LIMIT 20
  `;
  return Response.json({ mints });
}

async function getTokenClaims(sql: postgres.Sql) {
  const claims = await sql`
    SELECT * FROM "TokenClaim"
    ORDER BY "createdAt" DESC
    LIMIT 20
  `;
  return Response.json({ claims });
}

// Rarity queries
async function getRarities(sql: postgres.Sql) {
  const rarities = await sql`
    SELECT r.*, l."collectionName"
    FROM "Rarity" r
    LEFT JOIN "Lootbox" l ON l.id = r."lootboxId"
    ORDER BY r."createdAt" DESC
    LIMIT 20
  `;
  return Response.json({ rarities });
}

// Event queries
async function getEventTracking(sql: postgres.Sql) {
  const events = await sql`
    SELECT * FROM "EventTracking"
    ORDER BY "blockHeight" DESC
    LIMIT 50
  `;
  return Response.json({ events });
}

// VRF queries
async function getVRFCallbacks(sql: postgres.Sql) {
  const callbacks = await sql`
    SELECT * FROM "VRFCallback"
    ORDER BY "timestamp" DESC
    LIMIT 20
  `;
  return Response.json({ callbacks });
}

// Add these functions with the other query functions:

async function getLootboxPurchases(sql: postgres.Sql) {
  const purchases = await sql`
    SELECT 
      lp.*,
      l."collectionName",
      a.address as "buyerAddress"
    FROM "LootboxPurchase" lp
    INNER JOIN "Lootbox" l ON l.id = lp."lootboxId"
    INNER JOIN "Account" a ON a.address = lp."buyerAddress"
    ORDER BY lp."createdAt" DESC
    LIMIT 20
  `;
  return Response.json({ purchases });
}

async function getLootboxRewards(sql: postgres.Sql) {
  const rewards = await sql`
    SELECT 
      lr.*,
      lp."buyerAddress",
      l."collectionName"
    FROM "LootboxReward" lr
    INNER JOIN "LootboxPurchase" lp ON lp.id = lr."purchaseId"
    INNER JOIN "Lootbox" l ON l.id = lp."lootboxId"
    ORDER BY lr."createdAt" DESC
    LIMIT 20
  `;
  return Response.json({ rewards });
}

async function getTokenBalances(
  sql: postgres.Sql,
  filters: {
    address?: string | null;
    greaterThanZeroBalance: boolean;
  }
): Promise<Response> {
  try {
    const balances = await sql`
      SELECT 
        tb.*,
        t."tokenName",
        t."tokenUri",
        t.description,
        t.metadata,
        t."maxSupply",
        t."circulatingSupply",
        t."tokensBurned",
        t."propertyVersion",
        r."rarityName",
        r."weight" as "rarityWeight",
        a.address as "accountAddress"
      FROM "TokenBalance" tb
      INNER JOIN "Account" a ON a.address = tb."accountAddress"
      LEFT JOIN "Token" t ON t.id = tb."tokenId"
      LEFT JOIN "Rarity" r ON r.id = t."rarityId"
      WHERE 1=1
      ${filters.address ? sql`AND tb."accountAddress" = ${filters.address}` : sql``}
      ${filters.greaterThanZeroBalance ? sql`AND tb.balance > 0` : sql``}
      ORDER BY tb."lastUpdated" DESC
      ${!filters.address ? sql`LIMIT 20` : sql``}
    `;

    return Response.json({
      balances,
      totalCount: balances.length,
      filters
    });

  } catch (error) {
    console.error('Error in getTokenBalances:', error);
    return Response.json({
      error: 'Failed to fetch token balances',
      details: {
        filters,
        errorType: (error as Error).name
      }
    }, { status: 500 });
  }
}

async function getLootboxByCreatorAndCollection(sql: postgres.Sql, creator: string, collection: string) {
	const lootbox = await sql`
	  SELECT 
		l.id,
		l."creatorAddress",
		l."collectionResourceAddress",
		l."collectionName",
		l."collectionDescription",
		l."metadataUri",
		l.price,
		l."priceCoinType",
		l."maxStock",
		l."availableStock",
		l."isActive",
		l."isWhitelisted",
		l."autoTriggerWhitelistTime",
		l."autoTriggerActiveTime",
		l."totalVolume",
		l."purchaseCount",
		l."createdAt",
		l."updatedAt",
		l."timestamp",
		la.id as "analyticsId",
		la."volume24h",
		la."purchases24h",
		la."uniqueBuyers24h",
		la."updatedAt" as "analyticsUpdatedAt"
	  FROM "Lootbox" l
	  LEFT JOIN "LootboxAnalytics" la ON la."lootboxId" = l.id
	  WHERE l."creatorAddress" = ${creator}
	  AND l."collectionName" = ${collection}
	  LIMIT 1
	`;
	return Response.json({ lootbox: lootbox[0] });
  }

async function getRaritiesByLootboxId(sql: postgres.Sql, lootboxId: number) {
  const rarities = await sql`
    SELECT r.*, l."collectionName"
    FROM "Rarity" r
    LEFT JOIN "Lootbox" l ON l.id = r."lootboxId"
    WHERE r."lootboxId" = ${lootboxId}
    ORDER BY r."weight" DESC
  `;
  return Response.json({ rarities });
}

async function getRaritiesByCreatorAndCollection(sql: postgres.Sql, creator: string, collection: string) {
  const rarities = await sql`
    SELECT r.*, l."collectionName"
    FROM "Rarity" r
    INNER JOIN "Lootbox" l ON l.id = r."lootboxId"
    WHERE l."creatorAddress" = ${creator}
    AND l."collectionName" = ${collection}
    ORDER BY r."weight" DESC
  `;
  return Response.json({ rarities });
}

async function getTokensByLootboxId(sql: postgres.Sql, lootboxId: number) {
  const tokens = await sql`
    SELECT 
      t.*,
      r."rarityName",
      r."weight" as "rarityWeight",
      tc."name" as "collectionName"
    FROM "Token" t
    INNER JOIN "TokenCollection" tc ON tc.id = t."tokenCollectionId"
    LEFT JOIN "Rarity" r ON r.id = t."rarityId"
    WHERE tc.id = (
      SELECT "tokenCollectionId" 
      FROM "Lootbox" 
      WHERE id = ${lootboxId}
    )
    ORDER BY r."weight" DESC, t."tokenName" ASC
  `;
  return Response.json({ tokens });
}

async function getTokensByCreatorAndCollection(sql: postgres.Sql, creator: string, collection: string) {
  const tokens = await sql`
    SELECT 
      t.*,
      r."rarityName",
      r."weight" as "rarityWeight",
      tc."name" as "collectionName"
    FROM "Token" t
    INNER JOIN "TokenCollection" tc ON tc.id = t."tokenCollectionId"
    LEFT JOIN "Rarity" r ON r.id = t."rarityId"
    WHERE tc.creator = ${creator}
    AND tc.name = ${collection}
    ORDER BY r."weight" DESC, t."tokenName" ASC
  `;
  return Response.json({ tokens });
}

async function getTokensByLootboxCreator(sql: postgres.Sql, creatorAddress: string, collectionName: string) {
  const tokens = await sql`
    SELECT DISTINCT
      t.*,
      r."rarityName",
      r."weight" as "rarityWeight",
      tc."name" as "collectionName",
      l."creatorAddress" as "lootboxCreator",
      l."collectionResourceAddress"
    FROM "Token" t
    INNER JOIN "TokenCollection" tc ON tc.id = t."tokenCollectionId"
    LEFT JOIN "Rarity" r ON r.id = t."rarityId"
    INNER JOIN "Lootbox" l ON l."collectionResourceAddress" = tc.creator
    WHERE l."creatorAddress" = ${creatorAddress}
    AND l."collectionName" = ${collectionName}
    ORDER BY r."weight" DESC, t."tokenName" ASC
  `;
  return Response.json({ tokens });
}

async function getTokenCollectionByLootbox(sql: postgres.Sql, creatorAddress: string, collectionName: string) {
  const collection = await sql`
    SELECT DISTINCT
      tc.*,
      l."creatorAddress" as "lootboxCreator",
      l."collectionResourceAddress"
    FROM "TokenCollection" tc
    INNER JOIN "Lootbox" l ON l."collectionResourceAddress" = tc.creator
    WHERE l."creatorAddress" = ${creatorAddress}
    AND l."collectionName" = ${collectionName}
    LIMIT 1
  `;
  return Response.json({ collection: collection[0] });
}

// Add these query functions
async function getOffChainAccounts(sql: postgres.Sql) {
  const accounts = await sql`
    SELECT * FROM "OFFChain_Account"
    ORDER BY "createdAt" DESC
    LIMIT 20
  `;
  return Response.json({ accounts });
}

async function getOffChainAccount(sql: postgres.Sql, walletAddress: string) {
  const account = await sql`
    SELECT * FROM "OFFChain_Account"
    WHERE "walletAddress" = ${walletAddress}
    LIMIT 1
  `;
  return Response.json({ account: account[0] });
}

async function getLootboxStats(sql: postgres.Sql, lootboxId: number) {
  const stats = await sql`
    SELECT * FROM "OFFChain_LootboxStats"
    WHERE "lootboxId" = ${lootboxId}
    LIMIT 1
  `;
  return Response.json({ stats: stats[0] });
}

async function getLootboxLikes(sql: postgres.Sql, lootboxId: number) {
  const likes = await sql`
    SELECT l.*, a."walletAddress"
    FROM "OFFChain_LootboxLike" l
    INNER JOIN "OFFChain_LootboxStats" s ON s.id = l."lootboxStatsId"
    INNER JOIN "OFFChain_Account" a ON a.id = l."accountId"
    WHERE s."lootboxId" = ${lootboxId}
    ORDER BY l."createdAt" DESC
  `;
  return Response.json({ likes });
}

async function getLootboxViews(sql: postgres.Sql, lootboxId: number) {
  const views = await sql`
    SELECT v.*, a."walletAddress"
    FROM "OFFChain_LootboxView" v
    INNER JOIN "OFFChain_LootboxStats" s ON s.id = v."lootboxStatsId"
    INNER JOIN "OFFChain_Account" a ON a.id = v."accountId"
    WHERE s."lootboxId" = ${lootboxId}
    ORDER BY v."viewedAt" DESC
  `;
  return Response.json({ views });
}

async function getCreatorLootboxStats(sql: postgres.Sql, creatorAddress: string, includeLootboxes: boolean = false, includeTokens: boolean = false) {
  try {
    // Get basic stats first
    const stats = await sql`
      SELECT 
        s.*,
        l."collectionName",
        l."creatorAddress",
        l."tokenCollectionId"
      FROM "OFFChain_LootboxStats" s
      INNER JOIN "Lootbox" l ON l.id = s."lootboxId"
      WHERE l."creatorAddress" = ${creatorAddress}
      ORDER BY s."createdAt" DESC
    `;

    if (includeLootboxes || includeTokens) {
      // Get lootboxes in a separate query
      const lootboxDetails = includeLootboxes ? await sql`
        SELECT 
          l.*,
          tc.id as "tokenCollectionId"
        FROM "Lootbox" l
        LEFT JOIN "TokenCollection" tc ON tc.id = l."tokenCollectionId"
        WHERE l."creatorAddress" = ${creatorAddress}
      ` : [];

      // Get tokens in a separate query if needed
      const tokenCollections = includeTokens ? await sql`
        SELECT 
          tc.*,
          COALESCE(
            NULLIF(
              jsonb_agg(
                CASE WHEN t.id IS NOT NULL THEN
                  jsonb_build_object(
                    'id', t.id,
                    'tokenName', t."tokenName",
                    'tokenUri', t."tokenUri",
                    'maxSupply', t."maxSupply",
                    'circulatingSupply', t."circulatingSupply",
                    'tokensBurned', t."tokensBurned",
                    'propertyVersion', t."propertyVersion",
                    'rarityName', r."rarityName",
                    'rarityWeight', r."weight"
                  )
                ELSE NULL END
              ) FILTER (WHERE t.id IS NOT NULL),
              '[null]'
            ),
            '[]'
          )::jsonb as tokens
        FROM "TokenCollection" tc
        LEFT JOIN "Token" t ON t."tokenCollectionId" = tc.id
        LEFT JOIN "Rarity" r ON r.id = t."rarityId"
        WHERE tc.id IN (
          SELECT DISTINCT "tokenCollectionId" 
          FROM "Lootbox" 
          WHERE "creatorAddress" = ${creatorAddress}
          AND "tokenCollectionId" IS NOT NULL
        )
        GROUP BY tc.id
      ` : [];

      // Merge the data
      stats.forEach(stat => {
        if (includeLootboxes) {
          stat.lootbox = lootboxDetails.find(l => l.id === stat.lootboxId);
        }
        if (includeTokens && stat.tokenCollectionId) {
          stat.tokenCollection = tokenCollections.find(tc => tc.id === stat.tokenCollectionId);
        }
      });
    }

    return Response.json({
      stats,
      totalCount: stats.length
    });

  } catch (error) {
    console.error('Error in getCreatorLootboxStats:', error);
    return Response.json({
      error: (error as Error).message,
      details: {
        creatorAddress,
        errorType: (error as Error).name,
        fullError: (error as Error).toString()
      }
    }, { status: 500 });
  }
}

async function getLootboxStatsByUrl(sql: postgres.Sql, url: string, includeLootbox: boolean = false, includeTokens: boolean = false) {
  try {
    // Get basic stats first
    const stats = await sql`
      SELECT 
        s.*,
        l."collectionName",
        l."creatorAddress",
        l."tokenCollectionId"
      FROM "OFFChain_LootboxStats" s
      INNER JOIN "Lootbox" l ON l.id = s."lootboxId"
      WHERE s.url = ${url}
    `;

    if (!stats[0]) {
      return Response.json({ error: 'Stats not found for URL' }, { status: 404 });
    }

    if (includeLootbox || includeTokens) {
      // Get lootbox details if requested
      const lootboxDetails = includeLootbox ? await sql`
        SELECT l.*
        FROM "Lootbox" l
        WHERE l.id = ${stats[0].lootboxId}
      ` : [];

      // Get token collection and tokens if requested
      const tokenCollection = includeTokens && stats[0].tokenCollectionId ? await sql`
        SELECT 
          tc.*,
          COALESCE(
            NULLIF(
              jsonb_agg(
                CASE WHEN t.id IS NOT NULL THEN
                  jsonb_build_object(
                    'id', t.id,
                    'tokenName', t."tokenName",
                    'tokenUri', t."tokenUri",
                    'maxSupply', t."maxSupply",
                    'circulatingSupply', t."circulatingSupply",
                    'tokensBurned', t."tokensBurned",
                    'propertyVersion', t."propertyVersion",
                    'rarityName', r."rarityName",
                    'rarityWeight', r."weight"
                  )
                ELSE NULL END
              ) FILTER (WHERE t.id IS NOT NULL),
              '[null]'
            ),
            '[]'
          )::jsonb as tokens
        FROM "TokenCollection" tc
        LEFT JOIN "Token" t ON t."tokenCollectionId" = tc.id
        LEFT JOIN "Rarity" r ON r.id = t."rarityId"
        WHERE tc.id = ${stats[0].tokenCollectionId}
        GROUP BY tc.id
      ` : [];

      // Merge the data
      if (includeLootbox) {
        stats[0].lootbox = lootboxDetails[0];
      }
      if (includeTokens && tokenCollection[0]) {
        stats[0].tokenCollection = tokenCollection[0];
      }
    }

    return Response.json({ stats: stats[0] });

  } catch (error) {
    console.error('Error in getLootboxStatsByUrl:', error);
    return Response.json({
      error: (error as Error).message,
      details: {
        url,
        errorType: (error as Error).name,
        fullError: (error as Error).toString()
      }
    }, { status: 500 });
  }
}

async function checkLootboxStatsUrlExists(sql: postgres.Sql, url: string) {
  try {
    const result = await sql`
      SELECT EXISTS (
        SELECT 1 
        FROM "OFFChain_LootboxStats" 
        WHERE url = ${url}
      ) as "exists"
    `;
    
    return Response.json({ 
      exists: result[0].exists,
      url 
    });
  } catch (error) {
    console.error('Error in checkLootboxStatsUrlExists:', error);
    return Response.json({
      error: (error as Error).message,
      details: {
        url,
        errorType: (error as Error).name,
        fullError: (error as Error).toString()
      }
    }, { status: 500 });
  }
}

async function upsertOffChainAccount(sql: postgres.Sql, data: {
  walletAddress: string;
  email: string | null;
  username: string | null;
  preferences: Record<string, unknown> | null;
  lastLoginAt: Date;
}) {
  try {
    // Additional server-side validation
    if (!data.walletAddress || typeof data.walletAddress !== 'string') {
      throw new Error('Invalid wallet address');
    }

    if (data.email && typeof data.email !== 'string') {
      throw new Error('Invalid email format');
    }

    if (data.username && typeof data.username !== 'string') {
      throw new Error('Invalid username format');
    }

    // Sanitize preferences before storing
    const sanitizedPreferences = data.preferences ? JSON.stringify(data.preferences) : null;

    // Use parameterized query to prevent SQL injection
    const account = await sql`
      INSERT INTO "OFFChain_Account" (
        "walletAddress",
        "email",
        "username",
        "preferences",
        "lastLoginAt",
        "createdAt",
        "updatedAt"
      ) 
      VALUES (
        ${data.walletAddress},
        ${data.email},
        ${data.username},
        ${sanitizedPreferences}::jsonb,
        ${data.lastLoginAt},
        NOW(),
        NOW()
      )
      ON CONFLICT ("walletAddress") 
      DO UPDATE SET
        "email" = EXCLUDED."email",
        "username" = EXCLUDED."username",
        "preferences" = EXCLUDED."preferences",
        "lastLoginAt" = EXCLUDED."lastLoginAt",
        "updatedAt" = NOW()
      RETURNING 
        "id",
        "walletAddress",
        "email",
        "username",
        "preferences",
        "lastLoginAt",
        "createdAt",
        "updatedAt"
    `;

    // Only return necessary fields
    return Response.json({ 
      account: account[0],
      message: 'Account updated successfully'
    });

  } catch (error) {
    console.error('Error in upsertOffChainAccount:', error);
    
    // Handle specific database errors
    if ((error as Error).message.includes('unique constraint')) {
      if ((error as Error).message.toLowerCase().includes('email')) {
        return Response.json({ 
          error: 'Email already in use',
          code: 'EMAIL_TAKEN'
        }, { 
          status: 400,
          headers: {
            'Content-Type': 'application/json'
          }
        });
      }
      if ((error as Error).message.toLowerCase().includes('username')) {
        return Response.json({ 
          error: 'Username already taken',
          code: 'USERNAME_TAKEN'
        }, { 
          status: 400,
          headers: {
            'Content-Type': 'application/json'
          }
        });
      }
    }

    // Generic error response
    return Response.json({
      error: 'An error occurred while processing your request',
      code: 'INTERNAL_ERROR',
      details: {
        message: (error as Error).message,
        type: (error as Error).name
      }
    }, { 
      status: 500,
      headers: {
        'Content-Type': 'application/json'
      }
    });
  }
}

// Helper function to generate unique URL
async function generateUniqueUrl(sql: postgres.Sql, baseUrl: string): Promise<string> {
  const existing = await sql`
    SELECT EXISTS(
      SELECT 1 FROM "OFFChain_LootboxStats" WHERE url = ${baseUrl}
    ) as "exists"
  `;

  if (!existing[0].exists) return baseUrl;

  const randomSuffix = Math.random().toString().substring(2, 11);
  const newUrl = `${baseUrl}-${randomSuffix}`;

  const existingWithSuffix = await sql`
    SELECT EXISTS(
      SELECT 1 FROM "OFFChain_LootboxStats" WHERE url = ${newUrl}
    ) as "exists"
  `;

  return existingWithSuffix[0].exists ? generateUniqueUrl(sql, baseUrl) : newUrl;
}

// Main creation function
async function createOffchainLootboxStats(
  sql: postgres.Sql,
  data: {
    lootboxId: number;
    url: string;
    rarityColors: Record<string, string>;
    creatorAddress: string;
    collectionName: string;
  }
): Promise<Response> {
  try {
    // Verify lootbox ownership
    const lootboxResponse = await sql`
      SELECT id, "creatorAddress"
      FROM "Lootbox"
      WHERE id = ${data.lootboxId}
      AND "creatorAddress" = ${data.creatorAddress}
      AND "collectionName" = ${data.collectionName}
      LIMIT 1
    `;

    if (!lootboxResponse[0]) {
      return Response.json({ error: 'Invalid lootbox ID or ownership' }, { status: 400 });
    }

    // Check if record already exists
    const existing = await sql`
      SELECT id FROM "OFFChain_LootboxStats"
      WHERE "lootboxId" = ${data.lootboxId}
      LIMIT 1
    `;

    if (existing[0]) {
      return Response.json(
        { message: 'Offchain stats already exist' },
        { status: 409 }
      );
    }

    // Generate unique URL if needed
    const uniqueUrl = await generateUniqueUrl(sql, data.url);

    // Create the record
    const offchainLootbox = await sql`
      INSERT INTO "OFFChain_LootboxStats" (
        "lootboxId",
        "url",
        "rarityColorMap",
        "updatedAt",
        "createdAt"
      ) VALUES (
        ${data.lootboxId},
        ${uniqueUrl},
        ${JSON.stringify(data.rarityColors)},
        NOW(),
        NOW()
      )
      RETURNING *
    `;

    return Response.json({
      ...offchainLootbox[0],
      urlModified: uniqueUrl !== data.url
    });

  } catch (error) {
    console.error('Create offchain error:', error);
    return Response.json(
      { error: 'Failed to create offchain data' },
      { status: 500 }
    );
  }
}

async function getLootboxStatsByCollection(
  sql: postgres.Sql,
  creatorAddress: string,
  collectionName: string,
  includeLootbox: boolean = false,
  includeTokens: boolean = false
): Promise<Response> {
  try {
    // Get basic stats first
    const stats = await sql`
      SELECT 
        s.*,
        l."collectionName",
        l."creatorAddress",
        l."tokenCollectionId"
      FROM "OFFChain_LootboxStats" s
      INNER JOIN "Lootbox" l ON l.id = s."lootboxId"
      WHERE l."creatorAddress" = ${creatorAddress}
      AND l."collectionName" = ${collectionName}
      LIMIT 1
    `;

    if (!stats[0]) {
      return Response.json({ error: 'Stats not found' }, { status: 404 });
    }

    let result = stats[0];

    if (includeLootbox) {
      const lootbox = await sql`
        SELECT * FROM "Lootbox"
        WHERE "creatorAddress" = ${creatorAddress}
        AND "collectionName" = ${collectionName}
        LIMIT 1
      `;
      result.lootbox = lootbox[0];
    }

    if (includeTokens && result.tokenCollectionId) {
      const tokenCollection = await sql`
        SELECT 
          tc.*,
          COALESCE(
            jsonb_agg(
              jsonb_build_object(
                'id', t.id,
                'tokenName', t."tokenName",
                'tokenUri', t."tokenUri",
                'maxSupply', t."maxSupply",
                'circulatingSupply', t."circulatingSupply",
                'tokensBurned', t."tokensBurned",
                'propertyVersion', t."propertyVersion",
                'rarityName', r."rarityName",
                'rarityWeight', r."weight"
              )
            ) FILTER (WHERE t.id IS NOT NULL),
            '[]'
          )::jsonb as tokens
        FROM "TokenCollection" tc
        LEFT JOIN "Token" t ON t."tokenCollectionId" = tc.id
        LEFT JOIN "Rarity" r ON r.id = t."rarityId"
        WHERE tc.id = ${result.tokenCollectionId}
        GROUP BY tc.id
        LIMIT 1
      `;
      result.tokenCollection = tokenCollection[0];
    }

    return Response.json(result);

  } catch (error) {
    console.error('Error in getLootboxStatsByCollection:', error);
    return Response.json({
      error: 'Failed to fetch lootbox stats',
      details: {
        creatorAddress,
        collectionName,
        errorType: (error as Error).name
      }
    }, { status: 500 });
  }
}

async function getAllLootboxStats(
  sql: postgres.Sql,
  filters: {
    mustHaveUrl: boolean;
    isActive: boolean;
    isWhitelisted: boolean;
    includeTokenCollection: boolean;
    includeTokens: boolean;
  }
): Promise<Response> {
  try {
    const stats = await sql`
      SELECT 
        s.*,
        l."collectionName",
        l."creatorAddress",
        l."isActive",
        l."isWhitelisted",
        l."tokenCollectionId",
        l."collectionDescription",
        l."metadataUri"
      FROM "OFFChain_LootboxStats" s
      INNER JOIN "Lootbox" l ON l.id = s."lootboxId"
      WHERE 
        ${!filters.mustHaveUrl || sql`s.url IS NOT NULL`}
        AND ${!filters.isActive || sql`l."isActive" = true`}
        AND ${!filters.isWhitelisted || sql`l."isWhitelisted" = true`}
      ORDER BY s."createdAt" DESC
    `;

    if (filters.includeTokenCollection || filters.includeTokens) {
      // Get token collections
      const tokenCollectionsQuery = filters.includeTokens ? sql`
        SELECT 
          tc.id,
          tc.name,
          tc.creator,
          tc.description,
          tc."metadataUri",
          tc."createdAt",
          tc."updatedAt",
          COALESCE(
            jsonb_agg(
              jsonb_build_object(
                'id', t.id,
                'tokenName', t."tokenName",
                'tokenUri', t."tokenUri",
                'maxSupply', t."maxSupply",
                'circulatingSupply', t."circulatingSupply",
                'tokensBurned', t."tokensBurned",
                'propertyVersion', t."propertyVersion",
                'rarityName', r."rarityName",
                'rarityWeight', r."weight",
                'description', t.description,
                'metadata', t.metadata,
                'createdAt', t."createdAt",
                'updatedAt', t."updatedAt"
              ) ORDER BY r."weight" DESC NULLS LAST, t."tokenName" ASC
            ) FILTER (WHERE t.id IS NOT NULL),
            '[]'
          )::jsonb as tokens
        FROM "TokenCollection" tc
        LEFT JOIN "Token" t ON t."tokenCollectionId" = tc.id
        LEFT JOIN "Rarity" r ON r.id = t."rarityId"
        WHERE tc.id IN (
          SELECT DISTINCT "tokenCollectionId" 
          FROM "Lootbox" 
          WHERE id IN (${sql(stats.map(s => s.lootboxId))})
          AND "tokenCollectionId" IS NOT NULL
        )
        GROUP BY tc.id, tc.name, tc.creator, tc.description, tc."metadataUri", tc."createdAt", tc."updatedAt"
      ` : sql`
        SELECT 
          tc.id,
          tc.name,
          tc.creator,
          tc.description,
          tc."metadataUri",
          tc."createdAt",
          tc."updatedAt"
        FROM "TokenCollection" tc
        WHERE tc.id IN (
          SELECT DISTINCT "tokenCollectionId" 
          FROM "Lootbox" 
          WHERE id IN (${sql(stats.map(s => s.lootboxId))})
          AND "tokenCollectionId" IS NOT NULL
        )
      `;

      const tokenCollections = await tokenCollectionsQuery;

      // Map token collections to their respective stats
      stats.forEach(stat => {
        if (stat.tokenCollectionId) {
          stat.tokenCollection = tokenCollections.find(tc => tc.id === stat.tokenCollectionId);
        }
      });
    }

    return Response.json({
      stats,
      totalCount: stats.length,
      filters
    });

  } catch (error) {
    console.error('Error in getAllLootboxStats:', error);
    return Response.json({
      error: 'Failed to fetch lootbox stats',
      details: {
        filters,
        errorType: (error as Error).name
      }
    }, { status: 500 });
  }
}