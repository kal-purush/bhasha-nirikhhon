import { MongoClient, Db, Collection, Document, Filter, UpdateFilter, FindOptions } from 'mongodb';
import logger from '@utils/logger';
import { isDev } from '@utils/helpers';

// Left off @ right now metadata is implemented as a record in a collection: that's not great for obvious reasons
// can metadata be stored in a separate collection? or in a separate field in the same collection unrelated to the collection?
// My best guess is that metadata should be stored in a separate collection (probably the 'registered_indexes' collection), 
// but I'm not sure
// Yeah, metadata is technically only related to a region, not a collection, so registered_indexes should be 
// used to store/retrieve/update metadata for a region

class MongoDBService {
  private static instance: MongoDBService;
  private client: MongoClient;
  private db: Db;

  private constructor() {
    if (MongoDBService.instance) {
      throw new Error(
        'A MongoDBService instance has already been created. Use MongoDBService.getInstance() to get its instance',
      );
    }
    //                                      dev                        prod?
    const uri = process.env.MONGODB_URI || 'mongodb://localhost:27017' || 'mongodb://mongodb:27017';
    if (!uri) {
      throw new Error('MONGODB_URI environment variable is not set');
    }

    this.client = new MongoClient(uri);
  }

  public static getInstance(): MongoDBService {
    if (!MongoDBService.instance) {
      MongoDBService.instance = new MongoDBService();
    }
    return MongoDBService.instance;
  }

  public async connect(dbName: string = 'estatemetrics'): Promise<void> {
    await this.client.connect();
    this.db = this.client.db(dbName);
    logger.debug(`Connected to MongoDB database: ${dbName}`);
  }

  public async disconnect(): Promise<void> {
    await this.client.close();
    logger.debug('Disconnected from MongoDB');
  }

  public collection(name: string): Collection<Document> {

    return this.db.collection(name);
  }

  public validate = {
    dataset: (dataset: DatasetSchema) => {
      if (!dataset || !dataset.records || !Array.isArray(dataset.records)) {
        throw new Error(
          'Invalid dataset. A dataset must be at minimum an object with a records field that is an array.',
        );
      }
    },
  };

  public index = {
    create: async (indexName: string, records?: unknown[], _options?: { inferTypes: boolean }): Promise<void> => {
      const options = _options ?? { inferTypes: true };
      logger.debug(`Index ${indexName} creating index`);
      try {
        const collection = this.collection(indexName);
        await collection.createIndex({ '$**': 'text' });
        logger.debug(`Index ${indexName}: Index created`);
      } catch (error) {
        logger.error(`Index ${indexName}: An error occurred:`, error);
      }
    },

    upsert: async (
      indexName: string,
      dataset: DatasetSchema,
      _options?: { inferTypes: boolean },
    ): Promise<void> => {
      const options = _options ?? { inferTypes: true };
      this.validate.dataset(dataset);

      await this.index.create(indexName, dataset.records, options);

      const { meta, records } = dataset;

      if (meta) {
        await this.index.updateMetadata(indexName, meta);
      }

      if (records.length === 0) {
        return;
      }

      try {
        const collection = this.collection(indexName);
        const bulkOps = records.map((doc) => ({
          updateOne: {
            filter: { _id: doc.id },
            update: { $set: doc },
            upsert: true,
          },
        }));

        await collection.bulkWrite(bulkOps);
        logger.debug(`Index ${indexName}: Upserted ${records.length} documents`);
      } catch (error) {
        logger.error(`Index ${indexName}: Error upserting documents:`, error);
      }
    },

    delete: async (indexName: string, document: Document): Promise<void> => {
      try {
        const collection = this.collection(indexName);
        await collection.deleteOne({ _id: document._id });

        logger.debug(`Index ${indexName}: Document with ID ${document._id} deleted successfully`);
      } catch (error) {
        logger.error(`Index ${indexName}: Error deleting document ${JSON.stringify(document)}:`, error);
      }
    },

    deleteIndex: async (indexName: string): Promise<void> => {
      try {
        await this.db.dropCollection(indexName);
        logger.debug('Index deleted:', indexName);
      } catch (error: Error | any) {
        if (!error?.message?.includes('ns not found')) {
          console.error('Error deleting index:', error);
        }
      }
    },

    schema: async (indexName: string): Promise<Document | undefined> => {
      logger.debug(`Index ${indexName} fetching schema`);
      try {
        const collection = this.collection(indexName);
        const indexes = await collection.indexes();
        return indexes.find((index) => index.name === '$**_text');
      } catch (error) {
        logger.error('Error fetching index schema:', error);
      }
    },

    updateMetadata: async (indexName: string, newMetadata: Record<string, any>): Promise<void> => {
      logger.debug(`Index ${indexName}: Updating metadata`);
      try {
        await this.index.create(indexName);
        const collection = this.collection(indexName);
        await collection.updateOne(
          { _id: 'metadata' },
          { $set: newMetadata },
          { upsert: true },
        );
      } catch (error) {
        logger.error(`Index ${indexName}: Error updating metadata:`, error);
      }
    },
  };

  public data = {
    metadata: async (indexName: string): Promise<Record<string, any>> => {
      try {
        const collection = this.collection(indexName);
        const metadata = await collection.findOne({ _id: 'metadata' });
        return metadata ?? {};
      } catch (error) {
        return {};
      }
    },

    compute: async (
      indexName: string,
      _query?: Filter<Document>,
      _aggs?: Document,
    ): Promise<Document[]> => {
      const query = _query ?? {};
      const aggs = _aggs ?? {};

      try {
        const collection = this.collection(indexName);
        const pipeline = [
          { $match: query },
          { $group: aggs },
        ];
        const results = await collection.aggregate(pipeline).toArray();
        return results;
      } catch (error) {
        logger.error(`Index ${indexName}: Error computing aggregation:`, error);
        return [];
      }
    },

    query: async (
      indexName: string,
      body: Filter<Document>,
      size: number = 1000,
    ): Promise<Document[]> => {
      logger.debug(`MongoDB: querying data from ${indexName}`);
      const options: FindOptions = { limit: size };

      try {
        const collection = this.collection(indexName);
        const results = await collection.find(body, options).toArray();
        return results;
      } catch (error: any) {
        logger.error(`Index ${indexName}: Error fetching data:`, error);
        return [];
      }
    },

    get: async (
      indexName: string,
      query: Filter<Document> = {},
      size: number = 1000,
    ): Promise<DatasetSchema | void> => {
      logger.debug(`MongoDB: fetching data from ${indexName}`);
      try {
        const results = await this.data.query(indexName, query, size);
        const meta = await this.data.metadata(indexName);
        logger.debug(`MongoDB: returned data from ${indexName}`);
        return {
          records: results,
          meta,
        };
      } catch (error) {
        logger.error(`Index ${indexName}: Error fetching data:`, error);
      }
    },

    record: async (
      indexName: string,
      query: Filter<Document> = {},
      size: number = 1,
    ): Promise<Document | void> => {
      try {
        const collection = this.collection(indexName);
        const record = await collection.findOne(query);
        return record;
      } catch (error) {
        logger.error(`Index ${indexName}: Error fetching data:`, error);
      }
    },

    mutate: async (indexName: string, field: string): Promise<void> => {
      const { records } = await this.data.get(indexName);

      const mutatedRecords = records.map((record: Document) => {
        const recordDate = record[field];
        const epochTimestamp = new Date(recordDate).getTime();
        record.date = epochTimestamp;
        return record;
      });

      await this.index.upsert(indexName, { records: mutatedRecords });
    },
  };

  public utility = {
    dedupe: async (indexName: string, field: string): Promise<void> => {
      try {
        const collection = this.collection(indexName);
        const duplicates = await collection
          .aggregate([
            { $group: { _id: `$${field}`, count: { $sum: 1 }, docs: { $push: '$$ROOT' } } },
            { $match: { count: { $gt: 1 } } },
          ])
          .toArray();

        const bulkOps = duplicates.flatMap((group) => {
          const docsToRemove = group.docs.slice(1);
          return docsToRemove.map((doc) => ({
            deleteOne: { filter: { _id: doc._id } },
          }));
        });

        if (bulkOps.length > 0) {
          await collection.bulkWrite(bulkOps);
          logger.debug(`Deduplicated ${bulkOps.length} documents in collection: ${indexName}`);
        }
      } catch (error) {
        console.error('Error deduplicating documents:', error);
      }
    },
  };

  public async listIndices(): Promise<Record<string, any>> {
    const collections = await this.db.listCollections().toArray();
    const indexMap: Record<string, any> = {};

    for (const collection of collections) {
      const indexName = collection.name;
      const metadata = await this.data.metadata(indexName);
      indexMap[indexName] = metadata;
    }

    return indexMap;
  }

  public async query(indexNames: string | string[], query: string, size: number = 1000): Promise<Document[]> {
    const indexes = Array.isArray(indexNames) ? indexNames : [indexNames];
    logger.debug(`Querying index(es) ${indexes} with query: ${query}`);
    try {
      const results: Document[] = [];

      for (const indexName of indexes) {
        const collection = this.collection(indexName);
        const documents = await collection
          .find({ $text: { $search: query } }, { limit: size })
          .toArray();
        results.push(...documents);
      }

      logger.debug(`Query successful: ${indexes}`);
      return results;
    } catch (error) {
      logger.error(`Error querying index ${indexes}:`, error);
      return [];
    }
  }
}

// Interfaces and types
interface DatasetSchema {
  meta?: Document;
  records: Document[];
}

export default MongoDBService;