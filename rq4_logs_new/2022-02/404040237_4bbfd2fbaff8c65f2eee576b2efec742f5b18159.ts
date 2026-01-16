import { PromMetric } from '../prom/PromMetric';
import { HttpMetric } from './HttpMetric';

export const isHttpMetric = (metric: PromMetric): metric is HttpMetric => 'startRequestTimer' in metric;