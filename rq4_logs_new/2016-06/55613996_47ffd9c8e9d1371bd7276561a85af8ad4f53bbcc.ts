import { Query } from './core/query';
import { BrowserBridge } from './core/bridge';
import { Results } from './response-models';

export class FluxCapacitor {
  query: Query;
  bridge: BrowserBridge;
  results: Results;

  constructor(endpoint: string, config: any = {}) {
    this.bridge = new BrowserBridge(endpoint);
    this.query = new Query().withConfiguration(config);
  }

  nextPage() {
    new Pager(this).next();
  };

  lastPage() {
    new Pager(this).last();
  };

  search() {
    return this.bridge.search(this.query)
      .then(res => this.results = res);
  }
}

class Pager {
  constructor(private flux: FluxCapacitor) { }

  next() {
    this.paginate(true, this.hasNext);
  }

  last() {
    this.paginate(false, this.hasPrevious);
  }

  private paginate(forward: boolean, predicate: boolean) {
    const step = this.step(forward);
    if (predicate) {
      this.flux.query.skip(step);
      this.flux.search();
    }
  }

  private get hasNext(): boolean {
    return this.step(true) < this.total;
  }

  private get hasPrevious(): boolean {
    return this.lastStep !== 0;
  }

  private step(add: boolean): number {
    const records = this.flux.results.records.length;
    const skip = this.lastStep + (add ? records : -records);
    return skip >= 0 ? skip : 0;
  }

  private get lastStep(): number {
    return this.flux.results.pageInfo.recordStart - 1;
  }
  private get total(): number {
    return this.flux.results.totalRecordCount;
  }
}