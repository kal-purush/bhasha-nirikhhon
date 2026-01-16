import barChart from './bar-chart.vg.json';
import crossfilterFlights from './crossfilter-flights.vg.json';
import interactiveLegend from './interactive-legend.vg.json';
import overviewPlusDetail from './overview-plus-detail.vg.json';

const datasets = [
  barChart,
  crossfilterFlights,
  interactiveLegend,
  overviewPlusDetail
];

/**
 * Prefix dataset URLs to load it from extern
 */
datasets.forEach((ds) => {
  if(ds.data) {
    ds.data = ds.data.map((d) => {
      if(d.url) {
        d.url = 'https://vega.github.io/editor/' + d.url;
      }
      return d;
    });
  }
});

export default datasets;