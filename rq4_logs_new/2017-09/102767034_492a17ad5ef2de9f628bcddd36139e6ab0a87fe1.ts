import { ChartConfig, FieldMaps, stockGraph, panel, chartCursorSettings } from "app/shared.module/models/chart-vm";

export class ChartConfigs {

    static cursorSettings: chartCursorSettings = {
        valueBalloonsEnabled: true,
        fullWidth: true,
        cursorAlpha: 0.1,
        valueLineBalloonEnabled: true,
        valueLineEnabled: true,
        valueLineAlpha: 0.5
    }

    static chartGraph: stockGraph = {
        type: "candlestick",
        id: "g1",
        openField: "open",
        closeField: "close",
        highField: "high",
        lowField: "low",
        valueField: "close",
        // lineColor: "#00ff00",
        // fillColors: "#00ff00",
        // negativeLineColor: "#db4c3c",
        // negativeFillColors: "#db4c3c",
        fillAlphas: 1,
        comparedGraphLineThickness: 2,
        columnWidth: 0.6,
        useDataSetColors: false,
        comparable: true,
        compareField: "close",
        showBalloon: true,
        proCandlesticks: true
    };

    static stockPanel: panel = {
        title: "Value",
        percentHeight: 80,
        stockLegend: {
            valueTextRegular: undefined,
            periodValueTextComparing: "[[percents.value.close]]%"
        },
        stockGraphs: [ChartConfigs.chartGraph]
    };
    static volumeGraph: stockGraph =
    {
        valueField: "volume",
        // openField: "open",
        type: "column",
        showBalloon: true,
        fillAlphas: 1,
        // lineColor: "#0000ff",
        // fillColors: "#0000ff",
        // negativeLineColor: "#db4c3c",
        // negativeFillColors: "#db4c3c",
        useDataSetColors: true
    };
    static volumnePanel: panel = {

        title: "Volume",
        percentHeight: 20,
        marginTop: 1,
        columnWidth: 0.6,
        showCategoryAxis: false,
        valueAxes: [
            {
                usePrefixes: true
            }
        ],
        stockLegend: {
            markerType: "none",
            markerSize: 0,
            labelText: "",
            periodValueTextRegular: "[[value.close]]"
        },
        stockGraphs: [ChartConfigs.volumeGraph]
    }
    static StockChart: ChartConfig = {
        type: "stock",
        theme: "dark",
        dataDateFormat: "YYYY-MM-DD",
        chartScrollbarSettings: {
            graph: "g1"
        },
        chartCursorSettings: ChartConfigs.cursorSettings,
        dataSets: [],
        panels: [ChartConfigs.stockPanel, ChartConfigs.volumnePanel]
    }
}