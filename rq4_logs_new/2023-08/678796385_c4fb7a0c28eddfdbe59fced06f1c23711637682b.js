/**
 *
 * @typedef {{id:string,crypto_name:string,currency_histories:[{id:number,quoting:number,date:string}]}} Data
 */

/**
 *
 * @param {Data} data
 */
export const convertApiDataToChartJsFormat = (data) => {
  const labels = data.currency_histories.map((val) => val.date).reverse();
  const chartData = data.currency_histories.map((val) => val.quoting);
  return { labels, chartData };
};