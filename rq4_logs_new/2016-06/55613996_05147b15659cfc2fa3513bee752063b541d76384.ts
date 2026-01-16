import { Query } from '../core/query';
import { BrowserBridge } from '../core/bridge';
import { Results } from '../models/response';
import { Pager } from './pager';
import { SelectedValueRefinement, SelectedRangeRefinement } from '../models/request';

export class FluxCapacitor {
  private originalQuery: string = '';
  query: Query;
  bridge: BrowserBridge;
  results: Results;

  constructor(endpoint: string, config: any = {}) {
    this.bridge = new BrowserBridge(endpoint);
    this.query = new Query().withConfiguration(config);
  }

  nextPage() {
    return new Pager(this).next();
  }

  lastPage() {
    return new Pager(this).last();
  }

  search(query: string = this.originalQuery) {
    return this.bridge.search(this.query.withQuery(query))
      .then(res => {
        this.results = res;
        this.originalQuery = query;
      });
  }

  reset() {
    this.query = new Query(this.originalQuery);
    return this.search();
  }

  refine(refinement: SelectedValueRefinement | SelectedRangeRefinement) {
    this.query.withSelectedRefinements(refinement);
    return this.search();
  }

  unrefine(refinement: SelectedValueRefinement | SelectedRangeRefinement) {
    this.query.withoutSelectedRefinements(refinement);
    return this.search();
  }
}