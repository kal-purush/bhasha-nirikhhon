package demo;

import simulation.SimKAnonymity;
import boot.BootParams;

public class DemoKAnonymity {
	SimKAnonymity sim;

	public DemoKAnonymity(BootParams bootParams, double cellsize,
			double mtpScale, int interval, String directory) {
		sim = new SimKAnonymity(bootParams, cellsize, mtpScale, interval, directory);
	}

	public void run() {
		System.out.println("Simulation start wtih K anonymity!");
		/* check if countermeasure is k anonymity */
		if (!sim.getCountermeasure().equals("KANONYMITY")) {
			System.out.println("Countermeasure mismatch");
			return;
		}
		/* run query for once */
		sim.singleSimulation();
		/* run different queries for multiple times and compute average ic */ 
		sim.multipleSimulation();
		/* plot probability on google map and IC vs Q */
		sim.plot(sim.isFeasible(), sim.plotICvsQuery(), "KANONYMITY");
		/* send email */
		sim.sendEmail(sim.isFeasible(), sim.plotICvsQuery());
	}
}