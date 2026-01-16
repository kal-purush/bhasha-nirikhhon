import { ChartData, ChartOptions, ChartType } from "chart.js";

export const getChartConfig = (
  data: ChartData<"doughnut">,
  percentage: number,
  thickness: string,
  fontSize: string,
  circumference: number,
) => {
  return {
    data: data,
    options: {
      borderColor: "#BFBFBF",
      circumference: circumference,
      cutout: thickness, // Making it a doughnut chart (hole in the center)
      maintainAspectRatio: false, // Disable aspect ratio to allow custom size
      plugins: {
        // Add custom text plugin
        centerText: {
          text: percentage, // Text to display in the center
        },
        tooltip: {
          enabled: true,
        },
      },
      responsive: true, // for chart resizing
      rotation: 200,
    },
    plugins: [
      {
        beforeDraw(chart: {
          ctx: CanvasRenderingContext2D;
          chartArea: {
            top: number;
            bottom: number;
          };
          options: ChartOptions;
          width: number;
        }) {
          const { width } = chart;
          const ctx = chart.ctx;

          // get the coordinates of the center
          const centerX = width / 2;
          const centerY =
            chart.chartArea.top +
            (chart.chartArea.bottom - chart.chartArea.top) / 2;

          ctx.save(); // save the chart
          ctx.textAlign = "center"; // align text in middle
          ctx.textBaseline = "middle";
          ctx.font = `${fontSize} Arial`; // font for number
          ctx.fillStyle = "#4D6BDB"; // Text color
          ctx.fillText(percentage.toString(), centerX, centerY);
          ctx.restore();
        },
        id: "centerText",
      },
    ],

    type: "doughnut" as ChartType, // Explicitly casting to ChartType
  };
};