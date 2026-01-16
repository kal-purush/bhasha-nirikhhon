package org.oristool.qesm.printers;

import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.block.BlockBorder;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.chart.renderer.xy.XYLineAndShapeRenderer;
import org.jfree.chart.title.TextTitle;
import org.jfree.data.xy.XYDataset;

import javax.swing.BorderFactory;
import javax.swing.JFrame;
import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Font;

public class LineChartPrinter extends JFrame {

   public LineChartPrinter(XYDataset dataset) {
      this(dataset, "Title", "X", "Y");
   }

   public LineChartPrinter(XYDataset dataset, String title, String xLabel, String yLabel) {
      JFreeChart chart = createChart(dataset, title, xLabel, yLabel);

      ChartPanel chartPanel = new ChartPanel(chart);
      chartPanel.setBorder(BorderFactory.createEmptyBorder(15, 15, 15, 15));
      chartPanel.setBackground(Color.white);
      add(chartPanel);

      pack();
      setTitle(title);
      setLocationRelativeTo(null);
      setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
   }

   public void showChart() {
      setVisible(true);
   }

   private JFreeChart createChart(XYDataset dataset, String title, String xLabel, String yLabel) {

      JFreeChart chart = ChartFactory.createXYLineChart(
            title,
            xLabel,
            yLabel,
            dataset,
            PlotOrientation.VERTICAL,
            true,
            true,
            false);

      XYPlot plot = chart.getXYPlot();

      var renderer = new XYLineAndShapeRenderer();
      renderer.setSeriesPaint(0, Color.RED);
      renderer.setSeriesStroke(0, new BasicStroke(2.0f));

      plot.setRenderer(renderer);
      plot.setBackgroundPaint(Color.white);

      plot.setRangeGridlinesVisible(true);
      plot.setRangeGridlinePaint(Color.BLACK);

      plot.setDomainGridlinesVisible(true);
      plot.setDomainGridlinePaint(Color.BLACK);

      chart.getLegend().setFrame(BlockBorder.NONE);

      chart.setTitle(new TextTitle(title,
            new Font("Serif", java.awt.Font.BOLD, 18)));

      return chart;
   }
}