// server.js
import express from 'express';
import fetch from 'node-fetch';
import cors from 'cors';
import { MongoClient } from 'mongodb';
import dotenv from 'dotenv';

const app = express();
app.use(cors());
dotenv.config();

const BCHAIN_BASE = 'https://blockchain.info';
const CORS_QS = 'cors=true';
const DEFAULT_SUBSIDY_BTC = 3.125;
const BLOCKS_PER_DAY = 144;

// MongoDB Configuration
const MONGODB_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017';
const DB_NAME = 'bitcoin_hashprice';
const COLLECTIONS = {
  BLOCKS: 'blocks',
  HASHRATE: 'hashrate_history',
  CALCULATIONS: 'hashprice_calculations'
};

let db, mongoClient;

console.log('🔧 Server starting with config:', {
  BCHAIN_BASE,
  DEFAULT_SUBSIDY_BTC,
  BLOCKS_PER_DAY,
  MONGODB_URI: MONGODB_URI.replace(/\/\/.*@/, '//***@') // Hide credentials in logs
});

// MongoDB Connection and Schema Setup
async function initializeMongoDB() {
  console.log('📊 Connecting to MongoDB...');
  
  mongoClient = new MongoClient(MONGODB_URI);
  await mongoClient.connect();
  db = mongoClient.db(DB_NAME);
  
  console.log('✅ Connected to MongoDB');
  
  // Create collections with advanced indexing optimized for blockchain data
  console.log('🏗️  Setting up collections and indexes...');
  
  // Blocks collection - showcases MongoDB's strength with nested documents
  const blocksCollection = db.collection(COLLECTIONS.BLOCKS);
  
  // Compound indexes for efficient blockchain queries
  await blocksCollection.createIndexes([
    // Primary query patterns for blockchain data
    { key: { height: -1 }, name: 'height_desc', background: true },
    { key: { hash: 1 }, name: 'hash_unique', unique: true, background: true },
    { key: { timestamp: -1 }, name: 'timestamp_desc', background: true },
    
    // Advanced MongoDB features: Compound indexes for complex queries
    { key: { height: -1, timestamp: -1 }, name: 'height_time_compound', background: true },
    { key: { 'fee_stats.total_fees_btc': -1, height: -1 }, name: 'fees_height_compound', background: true },
    
    // Text index for searching transaction hashes (MongoDB's advantage over SQL)
    { key: { 'transactions.hash': 'text' }, name: 'tx_hash_text_search', background: true },
    
    // Geospatial-ready index (for future mining pool location data)
    { key: { 'mining_pool.location': '2dsphere' }, name: 'pool_location_geo', background: true, sparse: true },
    
    // TTL index for automatic cleanup (MongoDB exclusive feature)
    { key: { created_at: 1 }, name: 'ttl_cleanup', expireAfterSeconds: 7 * 24 * 3600, background: true } // 7 days
  ]);
  
  // Hashrate collection with time-series optimization
  const hashrateCollection = db.collection(COLLECTIONS.HASHRATE);
  await hashrateCollection.createIndexes([
    { key: { timestamp: -1 }, name: 'timestamp_desc', background: true },
    { key: { created_at: 1 }, expireAfterSeconds: 30 * 24 * 3600, background: true } // 30 days retention
  ]);
  
  // Calculations collection for caching results
  const calculationsCollection = db.collection(COLLECTIONS.CALCULATIONS);
  await calculationsCollection.createIndexes([
    { key: { calculated_at: -1 }, name: 'calc_time_desc', background: true },
    { key: { 'parameters.blocks_sampled': 1, calculated_at: -1 }, name: 'params_time_compound', background: true },
    { key: { created_at: 1 }, expireAfterSeconds: 24 * 3600, background: true } // 24 hours cache
  ]);
  
  console.log('✅ MongoDB indexes created successfully');
}

// [Previous helper functions remain the same: getJSON, getLatestBlock, getBlock, etc.]
async function getJSON(url) {
  console.log(`🌐 Fetching URL: ${url}`);
  try {
    const res = await fetch(url, { timeout: 30000 });
    const text = await res.text();
    
    if (!res.ok) {
      throw new Error(`HTTP ${res.status} ${res.statusText} for ${url}`);
    }
    
    if (text.trim().startsWith('<')) {
      throw new Error(`Invalid JSON from ${url}`);
    }
    
    return JSON.parse(text);
  } catch (err) {
    console.error(`❌ Fetch error for ${url}:`, err.message);
    throw err;
  }
}

async function getLatestBlock() {
  const url = `${BCHAIN_BASE}/latestblock?${CORS_QS}`;
  const result = await getJSON(url);
  console.log("✅ Fetched latest block:", { hash: result.hash, height: result.height });
  return result;
}

async function getBlock(hash) {
  const url = `${BCHAIN_BASE}/rawblock/${hash}?${CORS_QS}`;
  const result = await getJSON(url);
  console.log(`✅ Fetched block ${hash} (height: ${result.height})`);
  return result;
}

// Enhanced block processing with MongoDB document structure
async function processAndStoreBlocks(blocksToSample = 144) {
  console.log(`📦 Processing and storing last ${blocksToSample} blocks...`);
  
  const blocksCollection = db.collection(COLLECTIONS.BLOCKS);
  
  // Check if we already have recent data (MongoDB's efficient document queries)
  const latestStored = await blocksCollection.findOne(
    {}, 
    { sort: { height: -1 }, projection: { height: 1, hash: 1, timestamp: 1 } }
  );
  
  const currentLatest = await getLatestBlock();
  
  // Smart caching: only fetch if we don't have the latest block
  if (latestStored && latestStored.hash === currentLatest.hash) {
    console.log('💾 Using cached block data from MongoDB');
    const cachedBlocks = await blocksCollection
      .find({})
      .sort({ height: -1 })
      .limit(blocksToSample)
      .toArray();
    
    return cachedBlocks;
  }
  
  console.log('🔄 Fetching fresh block data...');
  
  // Fetch blocks with blockchain traversal
  let blocks = [];
  let currentHash = currentLatest.hash;
  
  for (let i = 0; i < blocksToSample; i++) {
    try {
      const blockData = await getBlock(currentHash);
      
      // Enhanced block document with MongoDB's flexible schema
      const blockDoc = {
        hash: blockData.hash,
        height: blockData.height,
        timestamp: blockData.time,
        prev_block: blockData.prev_block,
        size: blockData.size,
        tx_count: blockData.tx ? blockData.tx.length : 0,
        
        // Nested document structure (MongoDB's strength over SQL)
        fee_stats: {
          total_fees_sats: 0,
          total_fees_btc: 0,
          avg_fee_per_tx: 0,
          processed_tx_count: 0,
          missing_fee_count: 0
        },
        
        // Array of transaction summaries (impossible in SQL without complex joins)
        transactions: blockData.tx ? blockData.tx.map(tx => ({
          hash: tx.hash,
          fee: tx.fee || null,
          input_count: tx.inputs ? tx.inputs.length : 0,
          output_count: tx.out ? tx.out.length : 0,
          size: tx.size || null
        })) : [],
        
        // Embedded analytics (MongoDB's document model advantage)
        analytics: {
          fee_density: 0, // fees per KB
          tx_efficiency: 0, // tx per KB
          block_utilization: 0 // size vs max block size
        },
        
        // Metadata for data management
        created_at: new Date(),
        updated_at: new Date(),
        data_version: '1.0'
      };
      
      // Calculate fee statistics using MongoDB's document processing
      let totalFeesSats = 0;
      let processedTxCount = 0;
      let missingFeeCount = 0;
      
      for (const tx of blockData.tx || []) {
        if (typeof tx.fee === 'number') {
          totalFeesSats += tx.fee;
          processedTxCount++;
        } else {
          missingFeeCount++;
        }
      }
      
      // Update embedded statistics
      blockDoc.fee_stats = {
        total_fees_sats: totalFeesSats,
        total_fees_btc: totalFeesSats / 1e8,
        avg_fee_per_tx: processedTxCount > 0 ? totalFeesSats / processedTxCount : 0,
        processed_tx_count: processedTxCount,
        missing_fee_count: missingFeeCount
      };
      
      blockDoc.analytics = {
        fee_density: blockDoc.size > 0 ? totalFeesSats / blockDoc.size : 0,
        tx_efficiency: blockDoc.size > 0 ? blockDoc.tx_count / blockDoc.size : 0,
        block_utilization: blockDoc.size / 1000000 // assuming 1MB max block size
      };
      
      blocks.push(blockDoc);
      
      if (!blockData.prev_block) break;
      currentHash = blockData.prev_block;
      
    } catch (err) {
      console.error(`❌ Error processing block ${currentHash}:`, err.message);
      break;
    }
  }
  
  console.log(`📊 Processed ${blocks.length} blocks`);
  
  // MongoDB's atomic operations for data consistency
  const session = mongoClient.startSession();
  
  try {
    await session.withTransaction(async () => {
      // Remove old blocks beyond our window (MongoDB's flexible queries)
      const heightThreshold = blocks.length > 0 ? blocks[blocks.length - 1].height : 0;
      
      const deleteResult = await blocksCollection.deleteMany(
        { height: { $lt: heightThreshold } },
        { session }
      );
      
      console.log(`🗑️  Deleted ${deleteResult.deletedCount} old blocks`);
      
      // Upsert new blocks (MongoDB's powerful upsert capability)
      for (const block of blocks) {
        await blocksCollection.replaceOne(
          { hash: block.hash },
          block,
          { upsert: true, session }
        );
      }
      
      console.log(`💾 Stored ${blocks.length} blocks in MongoDB`);
    });
  } finally {
    await session.endSession();
  }
  
  return blocks;
}

// Enhanced hashrate storage with time-series capabilities
async function storeHashrateData() {
  console.log('⚡ Fetching and storing hashrate data...');
  
  const url = `https://api.blockchain.info/charts/hash-rate?timespan=1year&sampled=true&metadata=false&daysAverageString=7D&cors=true&format=json`;
  const data = await getJSON(url);
  
  if (!data.values || data.values.length === 0) {
    throw new Error('No hashrate data available');
  }
  
  const latest = data.values[data.values.length - 1];
  const hashrateDoc = {
    timestamp: latest.x,
    hashrate_ths: latest.y,
    hashrate_phs: latest.y / 1000,
    data_source: 'blockchain.info',
    created_at: new Date(),
    
    // Time-series metadata (MongoDB's strength for temporal data)
    time_series: {
      period: 'daily',
      aggregation: '7d_average',
      data_points_available: data.values.length
    }
  };
  
  // Store with automatic deduplication
  await db.collection(COLLECTIONS.HASHRATE).replaceOne(
    { timestamp: latest.x },
    hashrateDoc,
    { upsert: true }
  );
  
  console.log(`⚡ Stored hashrate: ${hashrateDoc.hashrate_phs.toFixed(2)} PH/s`);
  return hashrateDoc.hashrate_phs;
}

// MongoDB-powered hashprice calculation with caching
async function computeHashpriceBTCWithMongoDB({
  blocksToSample = 144,
  subsidyBTC = DEFAULT_SUBSIDY_BTC,
} = {}) {
  console.log('🚀 Starting MongoDB-powered hashprice calculation...');
  
  // Check for cached calculation (MongoDB's document-based caching)
  const calculationsCollection = db.collection(COLLECTIONS.CALCULATIONS);
  const recentCalculation = await calculationsCollection.findOne(
    {
      'parameters.blocks_sampled': blocksToSample,
      'parameters.subsidy_btc': subsidyBTC,
      calculated_at: { $gte: new Date(Date.now() - 5 * 60 * 1000) } // 5 minutes cache
    },
    { sort: { calculated_at: -1 } }
  );
  
  if (recentCalculation) {
    console.log('💾 Using cached hashprice calculation');
    return recentCalculation.result;
  }
  
  // Process and store blocks
  const blocks = await processAndStoreBlocks(blocksToSample);
  
  // MongoDB aggregation pipeline (showcases MongoDB's power over SQL)
  const feeAggregation = await db.collection(COLLECTIONS.BLOCKS).aggregate([
    { $sort: { height: -1 } },
    { $limit: blocksToSample },
    {
      $group: {
        _id: null,
        total_fees_btc: { $sum: '$fee_stats.total_fees_btc' },
        total_blocks: { $sum: 1 },
        avg_fee_per_block: { $avg: '$fee_stats.total_fees_btc' },
        max_fee_block: { $max: '$fee_stats.total_fees_btc' },
        min_fee_block: { $min: '$fee_stats.total_fees_btc' },
        total_transactions: { $sum: '$tx_count' },
        
        // Advanced analytics (impossible with simple SQL)
        fee_distribution: {
          $push: {
            height: '$height',
            fees: '$fee_stats.total_fees_btc',
            tx_count: '$tx_count',
            utilization: '$analytics.block_utilization'
          }
        }
      }
    }
  ]).toArray();
  
  const feeStats = feeAggregation[0];
  const hashratePHs = await storeHashrateData();
  
  // Calculate hashprice
  const feePerBlock = feeStats.avg_fee_per_block;
  const estFees144 = feePerBlock * BLOCKS_PER_DAY;
  const subsidyRevenue = BLOCKS_PER_DAY * subsidyBTC;
  const networkRevenueBTC = subsidyRevenue + estFees144;
  const hashpriceBTCPerPHPerDay = networkRevenueBTC / hashratePHs;
  
  const result = {
    hashpriceBTCPerPHPerDay,
    components: {
      blocksSampled: feeStats.total_blocks,
      subsidyBTCPerBlock: subsidyBTC,
      estFees144BTC: estFees144,
      networkRevenueBTCPerDay: networkRevenueBTC,
      networkHashratePHs: hashratePHs,
      
      // MongoDB-powered analytics
      advanced_stats: {
        fee_distribution: feeStats.fee_distribution,
        max_fee_block: feeStats.max_fee_block,
        min_fee_block: feeStats.min_fee_block,
        total_transactions: feeStats.total_transactions,
        avg_tx_per_block: feeStats.total_transactions / feeStats.total_blocks
      }
    }
  };
  
  // Cache the calculation result
  await calculationsCollection.insertOne({
    parameters: { blocks_sampled: blocksToSample, subsidy_btc: subsidyBTC },
    result,
    calculated_at: new Date(),
    created_at: new Date()
  });
  
  console.log('✅ Hashprice calculation completed and cached');
  return result;
}

// Enhanced REST endpoints
app.get('/hashprice-btc', async (req, res) => {
  const startTime = Date.now();
  console.log('🌐 /hashprice-btc endpoint called');
  
  try {
    const n = Number(req.query.n ?? '144');
    const subsidy = req.query.subsidy ? Number(req.query.subsidy) : DEFAULT_SUBSIDY_BTC;
    
    const finalN = Number.isFinite(n) && n > 0 ? Math.min(n, 288) : 144;
    const finalSubsidy = Number.isFinite(subsidy) && subsidy > 0 ? subsidy : DEFAULT_SUBSIDY_BTC;
    
    const result = await computeHashpriceBTCWithMongoDB({
      blocksToSample: finalN,
      subsidyBTC: finalSubsidy,
    });
    
    const response = {
      unit: 'BTC/PH/day',
      hashprice: result.hashpriceBTCPerPHPerDay,
      components: result.components,
      performance: {
        calculation_time_ms: Date.now() - startTime,
        data_source: 'mongodb_optimized'
      }
    };
    
    res.json(response);
    console.log(`✅ Response sent in ${Date.now() - startTime}ms`);
    
  } catch (e) {
    console.error(`❌ Error:`, e);
    res.status(500).json({ error: String(e) });
  }
});

// MongoDB-specific analytics endpoints (showcasing MongoDB advantages)
app.get('/blocks/analytics', async (req, res) => {
  try {
    const analytics = await db.collection(COLLECTIONS.BLOCKS).aggregate([
      { $sort: { height: -1 } },
      { $limit: 144 },
      {
        $facet: {
          fee_trends: [
            {
              $group: {
                _id: { $dateToString: { format: "%Y-%m-%d", date: { $toDate: { $multiply: ["$timestamp", 1000] } } } },
                avg_fees: { $avg: "$fee_stats.total_fees_btc" },
                block_count: { $sum: 1 }
              }
            },
            { $sort: { _id: 1 } }
          ],
          
          utilization_stats: [
            {
              $group: {
                _id: null,
                avg_utilization: { $avg: "$analytics.block_utilization" },
                max_utilization: { $max: "$analytics.block_utilization" },
                blocks_above_50pct: {
                  $sum: { $cond: [{ $gt: ["$analytics.block_utilization", 0.5] }, 1, 0] }
                }
              }
            }
          ],
          
          transaction_patterns: [
            {
              $group: {
                _id: {
                  tx_range: {
                    $switch: {
                      branches: [
                        { case: { $lt: ["$tx_count", 1000] }, then: "low" },
                        { case: { $lt: ["$tx_count", 2000] }, then: "medium" },
                        { case: { $gte: ["$tx_count", 2000] }, then: "high" }
                      ]
                    }
                  }
                },
                count: { $sum: 1 },
                avg_fees: { $avg: "$fee_stats.total_fees_btc" }
              }
            }
          ]
        }
      }
    ]).toArray();
    
    res.json({
      success: true,
      analytics: analytics[0],
      generated_at: new Date()
    });
    
  } catch (e) {
    res.status(500).json({ error: String(e) });
  }
});

// Server startup
async function startServer() {
  try {
    await initializeMongoDB();
    
    const PORT = process.env.PORT || 3000;
    app.listen(PORT, () => {
      console.log(`🚀 Server running on port ${PORT}`);
      console.log(`📡 Endpoints:`);
      console.log(`   - Hashprice: http://localhost:${PORT}/hashprice-btc`);
      console.log(`   - Analytics: http://localhost:${PORT}/blocks/analytics`);
      console.log(`💾 MongoDB features enabled: caching, indexing, analytics`);
    });
    
  } catch (error) {
    console.error('❌ Failed to start server:', error);
    process.exit(1);
  }
}

// Graceful shutdown
process.on('SIGINT', async () => {
  console.log('🛑 Shutting down gracefully...');
  if (mongoClient) {
    await mongoClient.close();
    console.log('✅ MongoDB connection closed');
  }
  process.exit(0);
});

startServer();
