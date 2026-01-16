import { EasyPieChart } from "../src/easyPieChart";

describe('Easy-Pie-Chart', () => {
  let instance: EasyPieChart;
  test("Create an EasyPieChart", () => {
    // Set up our document body
    document.body.innerHTML = '<div class="chart" id="canvas-chart" data-percent="100"></div>';
    instance = new EasyPieChart(document.getElementById("canvas-chart"), {
      barColor: "#69c",
      scaleColor: "#00378b",
      lineWidth: 10
    });
    expect(instance).toBeInstanceOf(EasyPieChart);
  });
});
