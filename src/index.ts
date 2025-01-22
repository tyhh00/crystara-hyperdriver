import postgres from "postgres";

export interface Env {
  // If you set another name in wrangler.toml as the value for 'binding',
  // replace "HYPERDRIVE" with the variable name you defined.
  HYPERDRIVE: Hyperdrive;
}

// Add CORS headers to all responses
const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
  'Access-Control-Max-Age': '86400',
};

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    // Handle OPTIONS request for CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, {
        headers: corsHeaders
      });
    }

    const sql = postgres(env.HYPERDRIVE.connectionString);
    const url = new URL(request.url);
    const path = url.pathname;

    try {
      const response = await handleRoute(path, sql, url);
      
      // Add CORS headers to the response
      Object.entries(corsHeaders).forEach(([key, value]) => {
        response.headers.set(key, value);
      });

      return response;
    } catch (error: any) {
      console.error('Error:', error);
      return new Response(
        JSON.stringify({ error: error.message }), 
        { 
          status: 500,
          headers: {
            'Content-Type': 'application/json',
            ...corsHeaders
          }
        }
      );
    } finally {
      await sql.end();
    }
  },
} satisfies ExportedHandler<Env>;

// Helper function to handle routes
async function handleRoute(path: string, sql: postgres.Sql, url: URL): Promise<Response> {
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
      return await getTokenBalances(sql);
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
      const lootboxId = url.searchParams.get('lootboxId');
      if (!lootboxId) return Response.json({ error: 'Lootbox ID required' }, { status: 400 });
      return await getTokensByLootboxId(sql, parseInt(lootboxId));
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
    SELECT l.*, la.*
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

async function getTokenBalances(sql: postgres.Sql) {
  const balances = await sql`
    SELECT 
      tb.*,
      t."tokenName",
      t."tokenUri",
      a.address as "accountAddress"
    FROM "TokenBalance" tb
    INNER JOIN "Account" a ON a.address = tb."accountAddress"
    LEFT JOIN "Token" t ON t.id = tb."tokenId"
    ORDER BY tb."lastUpdated" DESC
    LIMIT 20
  `;
  return Response.json({ balances });
}

async function getLootboxByCreatorAndCollection(sql: postgres.Sql, creator: string, collection: string) {
  const lootbox = await sql`
    SELECT l.*, la.*
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