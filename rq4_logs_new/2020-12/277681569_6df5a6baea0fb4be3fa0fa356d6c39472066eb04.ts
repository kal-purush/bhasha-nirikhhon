import * as am4core from '@amcharts/amcharts4/core';
import * as am4charts from '@amcharts/amcharts4/charts';
import am4themesKelly from '@amcharts/amcharts4/themes/kelly';
import am4themesAnimated from '@amcharts/amcharts4/themes/animated';
import graphColor from './Color';
import { TruepointTheme } from '../../../interfaces/TruepointTheme';

// @hwasurr - 2020.10.13 eslint error 정리 중
// any 타입 disable 처리. => 작성자@chanuuuu가 올바른 타입 정의 수정바랍니다.
// eslint-disable-next-line @typescript-eslint/explicit-module-boundary-types
export default function setComponent(data: any, theme: TruepointTheme): am4charts.XYChart {
  am4core.useTheme(am4themesKelly);
  am4core.useTheme(am4themesAnimated);

  const lightColor = am4core.color(theme.palette.primary.light);
  const darkColor = am4core.color(theme.palette.primary.dark);

  const chart: am4charts.XYChart = am4core.create('chartdiv', am4charts.XYChart);
  chart.data = data;
  chart.dateFormatter.inputDateFormat = 'i';
  chart.paddingRight = 15;
  chart.paddingLeft = 5;

  const valueAxis: any = chart.yAxes.push(new am4charts.ValueAxis());
  valueAxis.tooltip.disabled = true;
  valueAxis.renderer.labels.template.fill = am4core.color('#3a86ff');
  valueAxis.renderer.minWidth = 60;

  const valueAxis2: any = chart.yAxes.push(new am4charts.ValueAxis());
  valueAxis2.tooltip.disabled = true;
  valueAxis2.renderer.labels.template.fill = am4core.color('#b1ae71');
  valueAxis2.renderer.minWidth = 60;
  valueAxis2.syncWithAxis = valueAxis;

  const dateAxis = chart.xAxes.push(new am4charts.DateAxis());
  dateAxis.startLocation = 0.48;
  dateAxis.endLocation = 0.52;
  dateAxis.skipEmptyPeriods = true;
  dateAxis.tooltipDateFormat = 'yyyy-MM-dd HH:mm:ss';
  dateAxis.periodChangeDateFormats.setKey('day', '[bold]MM-dd[/]');
  dateAxis.renderer.labels.template.fill = am4core.color('#3a86ff');

  const dateAxis2 = chart.xAxes.push(new am4charts.DateAxis());
  dateAxis2.startLocation = 0.48;
  dateAxis2.endLocation = 0.52;
  dateAxis2.skipEmptyPeriods = true;
  dateAxis2.tooltipDateFormat = 'yyyy-MM-dd HH:mm:ss';
  dateAxis2.periodChangeDateFormats.setKey('day', '[bold]MM-dd[/]');
  dateAxis2.renderer.labels.template.fill = am4core.color('#b1ae71');

  // ****************************** base count series ***************************
  const series: any = chart.series.push(new am4charts.LineSeries());
  series.name = '기준 기간';
  series.xAxis = dateAxis;
  series.yAxis = valueAxis;
  series.dataFields.valueY = 'baseValue';
  series.groupFields.valueY = 'sum';
  series.dataFields.dateX = 'baseDate';
  series.tooltipText = '기준 기간: [bold]{baseValue}[/]';
  series.strokeWidth = 3;
  series.tensionX = 0.8;
  series.stroke = am4core.color('#3a86ff');
  // series.connect = true;
  series.hidden = true; // 기본 그래프 설정.
  series.toFront();

  // Drop-shaped tooltips
  series.tooltip.background.cornerRadius = 20;
  series.tooltip.background.strokeOpacity = 0;
  series.tooltip.pointerOrientation = 'vertical';
  series.tooltip.label.minWidth = 40;
  series.tooltip.label.minHeight = 40;
  series.tooltip.label.textAlign = 'middle';
  series.tooltip.label.textValign = 'middle';

  // ****************************** compare count series ***************************
  const newSeries = chart.series.push(new am4charts.LineSeries());
  newSeries.name = '비교 기간';
  newSeries.xAxis = dateAxis2;
  newSeries.yAxis = valueAxis2;
  newSeries.dataFields.valueY = 'compareValue';
  newSeries.dataFields.dateX = 'compareDate';
  if (newSeries.tooltip) {
    newSeries.tooltip.getFillFromObject = false;
    newSeries.tooltip.background.fill = am4core.color(graphColor.line);
  }
  newSeries.stroke = am4core.color('#b1ae71');
  newSeries.strokeWidth = 3;

  newSeries.tensionX = 0.8;
  newSeries.groupFields.valueY = 'sum';
  newSeries.tooltipText = '비교 기간 : [bold]{compareValue}[/]';
  newSeries.connect = true;
  newSeries.hidden = true; // 기본 그래프 설정.
  newSeries.toFront();

  // ****************************** cursor ***************************
  // Make a panning cursor
  chart.cursor = new am4charts.XYCursor();
  chart.cursor.xAxis = dateAxis;

  // ****************************** scroll bar ***************************
  // Create a horizontal scrollbar with previe and place it underneath the date axis
  const scrollbarX: any = new am4charts.XYChartScrollbar();
  scrollbarX.minHeight = 20;
  scrollbarX.startGrip.background.states.getKey('hover').properties.fill = lightColor;
  scrollbarX.endGrip.background.states.getKey('hover').properties.fill = lightColor;
  scrollbarX.thumb.background.states.getKey('hover').properties.fill = lightColor;
  scrollbarX.startGrip.background.states.getKey('down').properties.fill = darkColor;
  scrollbarX.endGrip.background.states.getKey('down').properties.fill = darkColor;
  scrollbarX.thumb.background.states.getKey('down').properties.fill = darkColor;

  chart.scrollbarX = scrollbarX;
  chart.scrollbarX.background.fill = lightColor;
  chart.scrollbarX.startGrip.background.fill = lightColor;
  chart.scrollbarX.endGrip.background.fill = lightColor;
  chart.scrollbarX.thumb.background.fill = lightColor;

  // dateAxis.start = 0.75;
  dateAxis.keepSelection = true;
  // And, for a good measure, let's add a legend
  const legend = new am4charts.Legend();
  legend.labels.template.fill = am4core.color(theme.palette.text.secondary);
  chart.legend = legend;

  return chart;
}