import {
  MongoClient,
  Collection,
  InsertWriteOpResult,
  UpdateWriteOpResult,
  DeleteWriteOpResultObject,
  UpdateManyOptions, FilterQuery, UpdateQuery
} from 'mongodb';

/** Database wrapper */
export default class Database {

  /**
   * Promise which connects to the mongodb server
   *
   * @type {Promise<MongoClient>}
   * @private
   */
  private connection: Promise<MongoClient>;

  /**
   * Name of the database
   *
   * @type {string}
   * @private
   */
  private dbName: string;

  /**
   * Creates an instance of Database
   *
   * @this {Database}
   * @param {string} url - mongodb adress
   * @constructor
   */
  constructor (url: string, dbName: string) {

    this.connection = MongoClient.connect(url, { useNewUrlParser: true });
    this.dbName = dbName;

  }

  /**
   * Returns the collection promise from the mongodb server
   *
   * @this {Database}
   * @param {string} collection - name of the collection what user needs
   * @private
   */
  private async getCollection (collection: string): Promise<Collection> {

    const db = (await this.connection).db(this.dbName);
    return db.collection(collection);

  }

  /**
   * Inserts elements in the database
   *
   * @this {Database}
   * @param {string} collectionName - name of the collection
   * @param {...object} elements - elements are need to be inserted
   * @param {Promise<InsertWriteOpResult>} - answer from mongodb
   */
  public async insert (collectionName: string, ...elements: object[]): Promise<InsertWriteOpResult> {

    const collection = await this.getCollection(collectionName);
    return collection.insertMany(elements);

  }

  /**
   * Finds elements in the database
   *
   * @this {Database}
   * @param {string} collectionName - name of the collection
   * @param {FilterQuery} query - filter for the collection
   * @returns {Promise<any[]>} Array of the finding objects
   */
  public async find (collectionName: string, query: FilterQuery<any> = {}): Promise<any[]> {

    const collection = await this.getCollection(collectionName);
    return collection.find(query).toArray();

  }

  /**
   * Updates elements in the database
   *
   * @this {Database}
   * @param {string} collectionName - name of the collection
   * @param {FilterQuery} query - filter for the collection
   * @param {UpdateQuery} updater - rules for the update
   * @param {UpdateManyOptions} options
   * @returns {Promise<UpdateWriteOpResult>} - answer from mongodb
   */
  public async update (
    collectionName: string,
    query: FilterQuery<any>,
    updater: UpdateQuery<any>,
    options?: UpdateManyOptions
  ): Promise<UpdateWriteOpResult> {

    const collection = await this.getCollection(collectionName);
    return collection.updateMany(query, updater, options);

  }

  /**
   * Removes elements in the database
   *
   * @this {Database}
   * @param {string} collectionName - name of the collection
   * @param {object} query - filter for the collection
   * @returns {Promise<WriteOpResult>} - answer from mongodb
   */
  public async remove (collectionName: string, query: object): Promise<DeleteWriteOpResultObject> {

    const collection = await this.getCollection(collectionName);
    return collection.deleteMany(query);

  }

  /**
   * Removes collection from the database
   *
   * @this {Database}
   * @param {string} collectionName - name of the collection
   * @async
   * @return {Promise<any>} - answer from mongodb
   */
  public async removeCollection (collectionName: string): Promise<any> {
    return (await this.getCollection(collectionName)).drop();
  }

  /**
   * Clears all the database
   *
   * @this {Database}
   * @async
   * @return {Promise<void>}
   */
  public async reset (): Promise<void> {
    return (await this.connection).db(this.dbName).dropDatabase();
  }

}