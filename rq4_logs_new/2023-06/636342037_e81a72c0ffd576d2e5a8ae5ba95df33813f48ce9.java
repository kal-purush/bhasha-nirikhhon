import org.jfree.chart.ChartFactory;
import org.jfree.chart.ChartPanel;
import org.jfree.chart.JFreeChart;
import org.jfree.chart.plot.PlotOrientation;
import org.jfree.chart.plot.XYPlot;
import org.jfree.data.xy.XYSeries;
import org.jfree.data.xy.XYSeriesCollection;

import javax.swing.*;
import java.awt.*;
import java.awt.geom.Ellipse2D;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Scanner;

public class EllipticCurve {
    // BigInteger потому что обычный mod неправильно работает с отрицательными числами
    public BigInteger a; // Коэффициент a
    public BigInteger b; // Коэффициент b
    public BigInteger p; // Модуль p
    public Point g; // Генерирующая точка g


    public EllipticCurve(int a, int b, int p, Point g) {
        this.a = BigInteger.valueOf(a);
        this.b = BigInteger.valueOf(b);
        this.p = BigInteger.valueOf(p);
        this.g = g;
    }

    // y^2 = x^3 + ax + b
    public ArrayList<Point> solves() {
        ArrayList<Point> points = new ArrayList<Point>();

        for (BigInteger x = BigInteger.ZERO; x.compareTo(p) < 0; x = x.add(BigInteger.ONE)) {
            for (BigInteger y = BigInteger.ZERO; y.compareTo(p) < 0; y = y.add(BigInteger.ONE)) {
                //x^3 + ax + b - y^2 = 0
                if (((x.pow(3).add(a.multiply(x)).add(b).subtract(y.pow(2))).mod(p)).equals(BigInteger.ZERO))
                    points.add(new Point(x, y));
            }
        }
        return points;
    }

    public static void main(String[] args) {
        EllipticCurve curve = new EllipticCurve(3, 5, 23, new Point(BigInteger.valueOf(15), BigInteger.valueOf(13)));
        ArrayList<Point> points = curve.solves();
        XYSeriesCollection dataset = new XYSeriesCollection();
        XYSeries series = new XYSeries("Точки");
        for (Point point : points) {
            series.add(point.x, point.y);
            System.out.println(point);
        }
        dataset.addSeries(series);
        JFreeChart chart = ChartFactory.createScatterPlot("Эллиптическая кривая", "X", "Y",
                dataset, PlotOrientation.VERTICAL, true, true, false);
        XYPlot plot = (XYPlot) chart.getPlot();
        Shape shape = new Ellipse2D.Double(-2.5, -2.5, 5, 5);
        Shape shape1 = new Ellipse2D.Double(-6, -6, 12, 12);
        plot.getRenderer().setSeriesShape(0, shape);
        plot.getRenderer().setSeriesPaint(0, Color.BLACK);
        plot.setBackgroundPaint(Color.WHITE);
        plot.setDomainGridlinePaint(Color.GRAY);
        plot.setRangeGridlinePaint(Color.GRAY);

        ChartPanel panel = new ChartPanel(chart);
        panel.setPreferredSize(new Dimension(800, 600));
        JFrame frame = new JFrame("Эллиптическая кривая");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setContentPane(panel);
        frame.pack();
        frame.setVisible(true);
        frame.setLocationRelativeTo(null);

        XYSeries multiSeries = multiply(points, curve.p, curve.a);
        chart.getXYPlot().getDataset();
        dataset.addSeries(multiSeries);
        plot.getRenderer().setSeriesShape(1, shape1);
        plot.getRenderer().setSeriesPaint(1, Color.RED);
        panel.repaint();

        Scanner in = new Scanner(System.in);

        System.out.print("Сложить две точки кривой\nД/Н ");
        String answer = in.nextLine();
        if (answer.equalsIgnoreCase("д")) {
            XYSeries[] seriesSum = sum(curve.p, curve.a);

            chart.getXYPlot().getDataset();
            dataset.addSeries(seriesSum[0]);
            dataset.addSeries(seriesSum[1]);

            plot.getRenderer().setSeriesShape(1, shape1);
            plot.getRenderer().setSeriesPaint(1, Color.GREEN);

            plot.getRenderer().setSeriesShape(2, shape1);
            plot.getRenderer().setSeriesPaint(2, Color.RED);

            panel.repaint();
        }
        in.close();
    }

    private static XYSeries multiply(ArrayList<Point> points, BigInteger module, BigInteger a) {
        Scanner in = new Scanner(System.in);
        System.out.print("Введите номер точки для удвоения: ");
        Point p = points.get(Integer.parseInt(in.nextLine()));
        System.out.print("Введите значение, на которую умножим точку: ");
        int value = Integer.parseInt(in.nextLine());
        Point r = p.multiply(BigInteger.valueOf(value), module, a);
        System.out.println("Оригинальная точка: " + p);
        System.out.println("Удвоенная точка: " + r);
        XYSeries series = new XYSeries("Удвоение точки кривой");
        series.add(r.x.intValue(), r.y.intValue());
        //in.close();

        return series;
    }

    private static XYSeries[] sum(BigInteger module, BigInteger a) {
        XYSeries sumSeries = new XYSeries("Точки для суммирования");
        XYSeries sumResSeries = new XYSeries("Результат сложения");

        Point p = new Point(BigInteger.valueOf(2), BigInteger.valueOf(10));
        Point q = new Point(BigInteger.valueOf(8), BigInteger.valueOf(14));

        sumSeries.add(p.x.intValue(), p.y.intValue());
        sumSeries.add(q.x.intValue(), q.y.intValue());

        Point r = p.add(q, module, a);
        sumResSeries.add(r.x.intValue(), r.y.intValue());

        System.out.println("P:" + p.toString());
        System.out.println("Q:" + q.toString());
        System.out.println("R:" + r.toString());

        return new XYSeries[]{sumSeries, sumResSeries};
    }
}