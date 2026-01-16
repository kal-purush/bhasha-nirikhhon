import { ChartLoader } from './chart-loader';

/**
 * Creates bar and line charts.
 */
export class ChartBar {
    /**
     * Chart bar constructor
     * @constructor
     * @param {Any} config the slider configuration
     * @param {Object} attrs the feature attributes
     */
    constructor(config: any, attrs: object) {
       // set chart options
        this.title = config.title;
        this.type = config.type;
        this.options = {
            scales: {
                xAxes: [],
                yAxes: []
            }
        };

        // set data options
        // TODO: deal with layer id... for now id must be 0. In relality, we will check if the id is the one provided by configuration
        const colors = config.options.colors === '' ? ChartLoader.defaultColors : config.options.colors.split(';');
        const layerData = config.layers.find(i => i.id === '0');
        this.setData(layerData, attrs, colors);

        // set labels options
        this.options.scales.xAxes.push(this.setAxis('xAxes', config.labelsLine.xAxis, attrs));
        this.options.scales.yAxes.push(this.setAxis('yAxes', config.labelsLine.yAxis, attrs));
    }

    /**
     * Set the chart data
     * @function setData
     * @param {Object} layerData the layer data provided by configuration
     * @param {Object} attrs the feature attributes
     * @param {String[]} colors the array of colors to use
     */
    setData(layerData: object, attrs: object, colors: string[]) {
        this.data = ChartLoader.parse(layerData, attrs);

        // for each dataset, set options
        for (let [i, dataset] of this.data.datasets.entries()) {
            dataset.backgroundColor = `${colors[i]}80`;
            dataset.borderColor = colors[i];
            dataset.borderWidth = 2;
        }
    }

    /**
     * Set the chart axis
     * @function setAxis
     * @param {String} axe the axis to set (xAxes or yAxes)
     * @param {Any} config the chart configuration
     * @param {Object} attrs the feature attributes
     * @return {Object} the axis object
     */
    setAxis(axe: string, config: any, attrs: object): { [k: string]: any } {
        let optsAxe: { [k: string]: any } = {};

        if (config.type === 'field' || config.type === 'config') {
            // get values from the configuration or field because it is a category axe
            const ticks = ChartLoader.getLabels(config, attrs);

            optsAxe.type = 'category';
            optsAxe.labels = ticks;
        } else if (config.type === 'linear') {
            optsAxe.type = 'linear';
        } else if (config.type === 'time') {
            optsAxe.type = 'time';
            optsAxe.distribution = 'serie';
        }

        // axe gridlines display
        if (axe === 'xAxes') {
            optsAxe.gridLines = {
                display: true,
                drawTicks: true,
                drawBorder: false,
                drawOnChartArea: false
            }
        }

        // axe ticks
        optsAxe.ticks = {
            autoSkip: true,
            autoSkipPadding: 100
        }

        // axe title
        optsAxe.scaleLabel = {
            display: true,
            labelString: config.title
        }

        return optsAxe;
    }
}

export interface ChartBar {
    options: any;
    type: string;
    data: any;
    title: string;
}