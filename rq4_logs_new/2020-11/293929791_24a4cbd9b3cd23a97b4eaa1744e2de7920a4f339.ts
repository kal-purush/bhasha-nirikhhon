import { PostgresGateway } from "../gateways/index";
import { Trade } from "../types";
// import memcached from 'memcached';

export default class Thoridal {
  db: PostgresGateway;

  constructor() {
    this.db = new PostgresGateway();
  }

  public async write(body: Trade): Promise<{}> {
    const id = await this.db.write(body);
    return id;
  }

  public async read(id: string): Promise<Trade> {
    const trade = await this.db.read(id);
    return trade;
  }

  public async readByTicker(ticker: string): Promise<Trade> {
    const trade = await this.db.readByTicker(ticker);
    return trade;
  }

  public async readByCompany(company: string): Promise<Trade> {
    const trade = await this.db.readByCompany(company);
    return trade;
  }

  public async readByDate(date: string): Promise<Trade> {
    const trade = await this.db.readByDate(date);
    return trade;
  }
}