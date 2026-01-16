import { getSalesOverviewChartData } from './chart-apis';
export const retrieveSalesOverviewChartData = async () => {
  const result = await getSalesOverviewChartData();
  return result;
};