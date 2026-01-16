import { OverviewState } from './overview-state';
import { PeersState } from './peers-states';
import { QuoteState } from './quote-state';
import { NewsState } from '../reducers/news-reducer';
import { KeyStatsState } from '../reducers/keystats-reducer';
import { TimeSeriesState } from '../reducers/time-series-reducer';

export interface ApplicationState {
  overview: OverviewState;
  peers: PeersState;
  quote: QuoteState;
  news: NewsState;
  keyStats: KeyStatsState;
  timeSeries: TimeSeriesState;
}