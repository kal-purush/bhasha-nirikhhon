import { Pool, PoolClient } from 'pg';
import MongoDBService from '@utils/mongo-db';
import logger from '@utils/logger';
import { config } from '@utils/helpers';

interface Schema {
  [key: string]: string;
}

interface Document {
  [key: string]: any;
}

interface Collection {
  name: string;
}

interface MongoDB {
  collection: (name: string) => any;
  listCollections: () => Promise<Collection[]>;
}

interface Transformer {
  (doc: Document): Document;
}

// Define schemas for different collections
const rentalSchema: Schema = {
  id: 'TEXT',
  beds: 'DOUBLE PRECISION',
  baths: 'DOUBLE PRECISION',
  description: 'TEXT',
  first_listed: 'TIMESTAMP',
  first_seen: 'TIMESTAMP',
  last_seen: 'TIMESTAMP',
  lat: 'DOUBLE PRECISION',
  lon: 'DOUBLE PRECISION',
  sqft: 'DOUBLE PRECISION',
  price: 'MONEY',
  address: 'TEXT',
  url: 'TEXT'
};

const propertySchema: Schema = {
  id: 'TEXT',
  beds: 'DOUBLE PRECISION',
  baths: 'DOUBLE PRECISION',
  description: 'TEXT',
  first_listed: 'TIMESTAMP',
  first_seen: 'TIMESTAMP',
  last_seen: 'TIMESTAMP',
  lat: 'DOUBLE PRECISION',
  lon: 'DOUBLE PRECISION',
  sqft: 'SMALLINT',
  price: 'MONEY',
  address: 'TEXT',
  url: 'TEXT',
  median_rent: 'MONEY',
  avg_rent: 'MONEY',
  costs_tax: 'MONEY',
  costs_maintenance: 'MONEY',
  costs_insurance: 'MONEY',
  costs_hoa: 'MONEY',
  costs_total: 'MONEY',
  returns_avg_net: 'MONEY',
  returns_avg_gross: 'MONEY',
  returns_avg_roi: 'MONEY',
  returns_avg_caprate: 'MONEY',
  returns_avg_cashFlow: 'MONEY',
  returns_avg_breakEvenYears: 'MONEY',
  returns_median_net: 'MONEY',
  returns_median_gross: 'MONEY',
  returns_median_roi: 'DOUBLE PRECISION',
  returns_median_caprate: 'DOUBLE PRECISION',
  returns_median_cashFlow: 'MONEY',
  returns_median_breakEvenYears: 'DOUBLE PRECISION'
};

const reportSchema: Schema = {
  index: 'TEXT',
  date: 'TIMESTAMP',
  beds: 'DOUBLE PRECISION',
  count: 'SMALLINT',
  avg_sqft: 'DOUBLE PRECISION',
  avg_rent: 'DOUBLE PRECISION',
  median_rent: 'MONEY',
  avg_rent_per_sqft: 'MONEY',
  median_rent_per_sqft: 'MONEY',
  type: 'TEXT',
  description: 'TEXT',
  rent_percentile_25: 'MONEY',
  rent_percentile_50: 'MONEY',
  rent_percentile_90: 'MONEY'
};

// Function to reset the database
async function resetDatabase(dbName = 'root'): Promise<void> {
  const pgPool = new Pool({ connectionString: config.POSTGRES_URI });

  try {
    const pgClient = await pgPool.connect();

    // Drop the existing database
    await pgClient.query(`DROP DATABASE IF EXISTS ${dbName}`);
    logger.info(`Database ${dbName} dropped successfully.`);

    // Create a new database
    await pgClient.query(`CREATE DATABASE ${dbName}`);
    logger.info(`Database ${dbName} created successfully.`);

    pgClient.release();
  } catch (error) {
    logger.error('Error resetting the database:', error);
  } finally {
    await pgPool.end();
  }
}

// Function to process collections with a transformer
async function processCollections(
  collections: Collection[],
  schema: Schema,
  pgClient: PoolClient,
  db: MongoDB,
  transformer: Transformer
): Promise<void> {
  for (const { name } of collections) {
    logger.info(`Processing collection: ${name}`);
    const collection = db.collection(name);
    let documents = await collection.find().toArray();
    const uniqueDocuments = new Map<string, Document>();

    documents = documents
      .filter(({ _id }: { _id: any }) => _id !== 'metadata')
      .map((doc: Document) => {
        const transformedDoc = transformer(doc);
        uniqueDocuments.set(doc._id, transformedDoc);
        return transformedDoc;
      })
      .filter((doc: Document, index: number, self: Document[]) => self.findIndex(d => d.id === doc.id) === index);

    documents = Array.from([...uniqueDocuments.values()]).flat();
    const columns = Object.entries(schema).map(([key, type]) => `${key} ${type}`).join(',');

    await pgClient.query(`DROP TABLE IF EXISTS ${name} CASCADE`);
    await pgClient.query(`CREATE TABLE ${name} (${columns})`);

    for (const document of documents) {
      const cols = Object.keys(document).map(key => `"${key}"`).join(', ');
      const values = Object.values(document);
      const placeholders = values.map((_, index) => `$${index + 1}`).join(', ');

      try {
        await pgClient.query(`INSERT INTO ${name} (${cols}) VALUES (${placeholders})`, values);
      } catch (error) {
        logger.error(`Error inserting into table ${name}:`, error);
      }
    }
  }
}

// Transformer for rental collections
const rentalTransformer: Transformer = (doc: Document) => ({
  id: doc._id,
  beds: doc.beds,
  baths: doc.baths,
  description: doc.description,
  first_listed: doc.firstListed,
  first_seen: doc.firstSeen,
  last_seen: doc.lastSeen,
  lat: doc.latLong.lat,
  lon: doc.latLong.lon,
  sqft: doc.sqft || doc.sqFt || doc.area,
  price: doc.price,
  address: doc.address,
  url: doc.url,
});

// Transformer for property collections
const propertyTransformer: Transformer = (doc: Document) => {
  const core: Document = {
    id: doc._id,
    beds: doc.beds,
    baths: doc.baths,
    description: doc.description ?? null,
    first_listed: doc.firstListed,
    first_seen: doc.firstSeen,
    last_seen: doc.lastSeen,
    lat: doc.latLong.lat,
    lon: doc.latLong.lon,
    sqft: doc.sqFt ?? doc.area,
    price: doc.price,
    address: doc.address,
    url: doc.url,
    median_rent: doc.medianRent,
    avg_rent: doc.avgRent,
  };

  // Copy over costs
  Object.keys(doc.costs).forEach(key => {
    core[`costs_${key.toLowerCase()}`] = doc.costs[key];
  });

  // Copy over returns
  Object.keys(doc.returns.avg).forEach(key => {
    core[`returns_avg_${key.toLowerCase()}`] = doc.returns.avg[key];
  });
  Object.keys(doc.returns.median).forEach(key => {
    core[`returns_median_${key.toLowerCase()}`] = doc.returns.median[key];
  });

  return core;
};

// Transformer for reports
const reportTransformer: Transformer = (report: Document) => {
  return Object.values(report)
    .filter((r: any) => r && typeof r !== 'string')
    .map((d: Document) => {
      const { rentPercentiles, rentPerSqftPercentiles, avgSqFt, avgRent, medianRent, avgRentPerSqft, medianRentPerSqft, ...core } = d;
      return {
        ...core,
        avg_sqft: avgSqFt,
        avg_rent: avgRent,
        median_rent: medianRent,
        avg_rent_per_sqft: avgRentPerSqft,
        median_rent_per_sqft: medianRentPerSqft,
        rent_percentile_25: rentPercentiles['25th'],
        rent_percentile_50: rentPercentiles['50th'],
        rent_percentile_90: rentPercentiles['90th'],
      };
    });
};

// Function to sync data from MongoDB to PostgreSQL
export async function syncToPostgres(): Promise<void> {
  const mongoClient = MongoDBService.getInstance();
  const pgPool = new Pool({ connectionString: config.POSTGRES_URI });

  const pgClient = await pgPool.connect();

  try {
    // Clear all tables
    await clearAllTables(pgClient);

    const db = mongoClient.db;
    const collections = await db.listCollections().toArray();
    const rentalCollections = collections.filter(c => c.name.endsWith('_rentals'));
    const propertyCollections = collections.filter(c => c.name.endsWith('_properties'));
    const reports = collections.filter(c => c.name.endsWith('_report'));

    logger.info(`Found ${collections.length} collections in MongoDB`);

    // Process collections
    await processCollections(reports, reportSchema, pgClient, db, reportTransformer);
    await processCollections(rentalCollections, rentalSchema, pgClient, db, rentalTransformer);
    await processCollections(propertyCollections, propertySchema, pgClient, db, propertyTransformer);

    logger.info('Data processing completed successfully.');
  } catch (error) {
    logger.error('Error processing collections:', error);
  } finally {
    await pgClient.release();
    await pgPool.end();
  }
}

// Function to clear all tables in the PostgreSQL database
async function clearAllTables(pgClient: PoolClient): Promise<void> {
  const dropTablesQuery = `
    DO $$ 
    DECLARE 
      r RECORD;
    BEGIN 
      FOR r IN (SELECT tablename FROM pg_tables WHERE schemaname = current_schema()) LOOP 
        EXECUTE 'DROP TABLE IF EXISTS ' || quote_ident(r.tablename) || ' CASCADE'; 
      END LOOP; 
    END $$;
  `;

  try {
    await pgClient.query(dropTablesQuery);
    await pgClient.query(`DROP SCHEMA IF EXISTS root CASCADE`);
    await pgClient.query(`CREATE SCHEMA root`);
    await pgClient.query(`DROP SCHEMA IF EXISTS public CASCADE`);
    await pgClient.query(`CREATE SCHEMA public`);
    logger.info('All tables cleared successfully.');
  } catch (error) {
    logger.error('Error clearing tables:', error);
  }
}