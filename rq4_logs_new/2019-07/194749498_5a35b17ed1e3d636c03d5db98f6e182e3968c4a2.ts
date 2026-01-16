import { Quote, KeyStats } from '../../util';
import { TimePoint } from '../../util';

/**TODO FINISH SELECTOR */

interface Graph {
  chartData: {
    '1m': TimePoint[];
    '1y': TimePoint[];
    '1d': TimePoint[];
    '5d': TimePoint[];
    '5y': TimePoint[];
    ['max']: TimePoint[];
  };
}

export function dataSelector({ chartData }: Graph) {
  const oneMonth = chartData['1m'];
  const oneYeaer = chartData['1y'];
  const fiveYear = chartData['5y'];
  const oneDay = chartData['1d'];
  const fiveDay = chartData['5d'];

  return chartData
    ? {
        '1m': oneMonth.filter(graphData => {}),
      }
    : {};
}