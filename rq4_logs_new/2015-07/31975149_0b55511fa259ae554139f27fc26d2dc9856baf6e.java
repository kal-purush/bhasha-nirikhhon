package demo;

import simulation.SimAdditiveNoise;
import boot.BootParams;

public class DemoAdditiveNoise {
	SimAdditiveNoise sim;

	public DemoAdditiveNoise(BootParams bp, double cs, double scale, int inter, String dir) {
		sim = new SimAdditiveNoise(bp, cs, scale, inter, dir);
	}

	public void run() {
		System.out.println("Simulation start wtih addtive noise!");
		/* check if countermeasure is additive noise */
		if (!sim.getCountermeasure().equals("ADDITIVENOISE")) return;
		/* run query for once */
		sim.singleSimulation();
		/* run different queries for multiple times and compute average ic */ 
		if (sim.isFeasible()) sim.multipleSimulation();
		/* print probability and location of pu for each channel */
		if (sim.isFeasible()) sim.printSingle();
		/* print average ic for different queries */
		if (sim.isFeasible()) sim.printMultiple();
		/* plot probability on google map and IC vs Q */
		if (sim.isFeasible()) sim.plot();
		/* send email */
		sim.sendEmail();
	}
}