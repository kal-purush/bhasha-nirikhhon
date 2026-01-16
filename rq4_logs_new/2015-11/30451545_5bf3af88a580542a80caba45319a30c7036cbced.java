package controllers;
 
import javax.annotation.PostConstruct;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import javax.faces.bean.ManagedBean;
import org.primefaces.model.chart.Axis;
import org.primefaces.model.chart.AxisType;
import org.primefaces.model.chart.CategoryAxis;
import org.primefaces.model.chart.LineChartModel;
import org.primefaces.model.chart.ChartSeries;
import org.primefaces.model.chart.LineChartSeries;
import org.primefaces.model.chart.MeterGaugeChartModel;
import org.primefaces.model.chart.PieChartModel;
 
@ManagedBean
public class ChartView implements Serializable {
 
    private LineChartModel lineModel1;
    private LineChartModel lineModel2;
    private MeterGaugeChartModel meterGaugeModel1;
    private PieChartModel pieModel1;
     
    @PostConstruct
    public void init() {
        createLineModels();
        createMeterGaugeModels();
        createPieModels();
    }
 
    public PieChartModel getPieModel1() {
        return pieModel1;
    }
     
    private void createPieModels() {
        createPieModel1();
    }
    
    private void createPieModel1() {
        pieModel1 = new PieChartModel();
         
        pieModel1.set("Expired", 54);
        pieModel1.set("Due this Week", 15);
        pieModel1.set("Close to Expire", 180);
         
        pieModel1.setTitle("Police Checks");
        pieModel1.setLegendPosition("w");
        pieModel1.setDiameter(150);
    }
    
    public MeterGaugeChartModel getMeterGaugeModel1() {
        return meterGaugeModel1;
    }
    
    public LineChartModel getLineModel1() {
        return lineModel1;
    }
 
    public LineChartModel getLineModel2() {
        return lineModel2;
    }
    
    private MeterGaugeChartModel initMeterGaugeModel() {
        List<Number> intervals = new ArrayList<Number>(){{
            add(20);
            add(50);
            add(120);
            add(220);
        }};
         
        return new MeterGaugeChartModel(140, intervals);
    }
        
    private void createMeterGaugeModels() {
        meterGaugeModel1 = initMeterGaugeModel();
        meterGaugeModel1.setTitle("# Requisitions per Month");
        meterGaugeModel1.setGaugeLabel("Req/Month");
        meterGaugeModel1.setGaugeLabelPosition("bottom");
//        meterGaugeModel1.setShowTickLabels(false);
//        meterGaugeModel1.setLabelHeightAdjust(110);
//        meterGaugeModel1.setIntervalOuterRadius(100);
    }
    
    private void createLineModels() {
        lineModel1 = (LineChartModel) initLinearModel();
        lineModel1.setTitle("# Infra Calls");
        lineModel1.setLegendPosition("s");
        lineModel1.getAxes().put(AxisType.X, new CategoryAxis("Week"));
        Axis yAxis = lineModel1.getAxis(AxisType.Y);
        yAxis.setLabel("# Tickets");
        yAxis.setMin(0);
        yAxis.setMax(500);
         
        lineModel2 = initCategoryModel();
        lineModel2.setTitle("Category Chart");
        lineModel2.setLegendPosition("e");
        lineModel2.setShowPointLabels(true);
        lineModel2.getAxes().put(AxisType.X, new CategoryAxis("Years"));
        yAxis = lineModel2.getAxis(AxisType.Y);
        yAxis.setLabel("Births");
        yAxis.setMin(0);
        yAxis.setMax(200);
    }
     
    private LineChartModel initLinearModel() {
        LineChartModel model = new LineChartModel();
 
        LineChartSeries series1 = new LineChartSeries();
        series1.setLabel("September");
 
        series1.set("1", 200);
        series1.set("2", 375);
        series1.set("3", 298);
        series1.set("4", 401);
        series1.set("5", 89);
 
        LineChartSeries series2 = new LineChartSeries();
        series2.setLabel("August");
 
        series2.set("1", 150);
        series2.set("2", 285);
        series2.set("3", 410);
        series2.set("4", 328);
        series2.set("5", 284);
 
        model.addSeries(series1);
        model.addSeries(series2);
         
        return model;
    }
     
    private LineChartModel initCategoryModel() {
        LineChartModel model = new LineChartModel();
 
        ChartSeries boys = new ChartSeries();
        boys.setLabel("Boys");
        boys.set("2004", 120);
        boys.set("2005", 100);
        boys.set("2006", 44);
        boys.set("2007", 150);
        boys.set("2008", 25);
 
        ChartSeries girls = new ChartSeries();
        girls.setLabel("Girls");
        girls.set("2004", 52);
        girls.set("2005", 60);
        girls.set("2006", 110);
        girls.set("2007", 90);
        girls.set("2008", 120);
 
        model.addSeries(boys);
        model.addSeries(girls);
         
        return model;
    }
 
}