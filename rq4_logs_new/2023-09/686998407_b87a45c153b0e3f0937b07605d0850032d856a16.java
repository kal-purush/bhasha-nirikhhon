package jfx.fractal.explorer.drawing.lsystem;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnore;

import jfx.fractal.explorer.JFXFractalExplorer;

public class LSystem {
	private String name;
	private String axiom;
	private double angle;
	private double length;
	private int iterations;
	private double startX;
	private double startY;
	private double lengthFactor = 1.0;
	private JFXFractalExplorer jfxFractalExplorer;
	private List<LSystemRule> rules = new ArrayList<>();
	
	public LSystem() {
		
	}
	
	public LSystem(JFXFractalExplorer jfxFractalExplorer) {
		this.jfxFractalExplorer = jfxFractalExplorer;
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public String getAxiom() {
		return axiom;
	}
	
	public void setAxiom(String axiom) {
		this.axiom = axiom;
	}

	public double getLengthFactor() {
		return lengthFactor;
	}

	public void setLengthFactor(double lengthFactor) {
		this.lengthFactor = lengthFactor;
	}

	public double getAngle() {
		return angle;
	}
	
	public void setAngle(double angle) {
		this.angle = angle;
	}
	
	public double getLength() {
		return length;
	}
	
	public void setLength(double length) {
		this.length = length;
	}
	
	public int getIterations() {
		return iterations;
	}
	
	public void setIterations(int iterations) {
		this.iterations = iterations;
	}
	
	public double getStartX() {
		return startX;
	}
	
	public void setStartX(double startX) {
		this.startX = startX;
	}
	
	public double getStartY() {
		return startY;
	}
	
	public void setStartY(double startY) {
		this.startY = startY;
	}
	
	public List<LSystemRule> getRules() {
		return rules;
	}

	public void addRule(LSystemRule rule) {
		rules.add(rule);
	}
	
	@JsonIgnore
	public String getGeneration() {
		String generation = axiom;
		if(iterations == 0) {
			return generation;
		}
		
		for(int i = 0; i<iterations;i++) {
			generation = expand(generation);
			double progress = ((double)(i+1)/(double)iterations*100.0);
			if(jfxFractalExplorer!= null) {
				jfxFractalExplorer.updateStatusMessage("Producing LSystem Generation iteration " + i);
				jfxFractalExplorer.updateProgress(progress);
			}
		}
		
		return generation;
	}
	
	@Override
	public String toString() {
		return name;
	}
	
	private String expand(String genration) {
		if(genration == null || genration.isEmpty()) {
			return "";
		}
		String expandedString = "";
		for(char c : genration.toCharArray()) {
			String key =  String.valueOf(c);
			LSystemRule rule = getRule(key);
			if(rule != null) {
				expandedString = expandedString + rule.getReplacement();
			}else {
				expandedString = expandedString + key;
			}
		}
		
		return expandedString;
	}
	
	private LSystemRule getRule(String key) {
		if(rules == null || rules.size() == 0) {
			return null;
		}
		
		for(LSystemRule rule : rules) {
			if(rule.getKey().equals(key)) {
				return rule;
			}
		}
		
		return null;
	}
	
	@Override
	public LSystem clone()  {
		LSystem lsystem = new LSystem();
		lsystem.setAngle(angle);
		lsystem.setAxiom(axiom);
		lsystem.setIterations(iterations);
		lsystem.setLength(length);
		lsystem.setLengthFactor(lengthFactor);
		lsystem.setName(name);
		lsystem.setStartX(startX);
		lsystem.setStartY(startY);
		for(LSystemRule rule : rules) {
			lsystem.rules.add(rule.clone());
		}
		return lsystem;
	}
}