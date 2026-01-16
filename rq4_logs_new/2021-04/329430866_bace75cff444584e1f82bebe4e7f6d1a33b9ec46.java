package org.kpmp.umap;

import java.util.ArrayList;
import java.util.List;

public class FeatureData {

	private List<Double> xValues = new ArrayList<>();
	private List<Double> yValues = new ArrayList<>();
	private List<Double> expression = new ArrayList<>();

	public List<Double> getXValues() {
		return xValues;
	}

	public void setXValues(List<Double> x) {
		this.xValues = x;
	}

	public void addXValue(Double xValue) {
		xValues.add(xValue);
	}

	public List<Double> getYValues() {
		return yValues;
	}

	public void setYValues(List<Double> y) {
		this.yValues = y;
	}

	public void addYValue(Double yValue) {
		yValues.add(yValue);
	}

	public List<Double> getExpression() {
		return expression;
	}

	public void setExpression(List<Double> expression) {
		this.expression = expression;
	}

	public void addExpression(Double expressionValue) {
		this.expression.add(expressionValue);
	}

}