package demo;

import simulation.SimKClustering;
import boot.BootParams;

public class DemoKClustering {
	SimKClustering sim;

	public DemoKClustering(BootParams bootParams, double cellsize,
			double mtpScale, int interval, String directory) {
		sim = new SimKClustering(bootParams, cellsize, mtpScale, interval, directory);
	}

	public void run() {
		System.out.println("Simulation start wtih K clustering!");
		/* check if countermeasure is k anonymity */
		if (!sim.getCountermeasure().equals("KCLUSTERING")) {
			System.out.println("Countermeasure mismatch");
			return;
		}
		/* run query for once */
		sim.singleSimulation();
		/* run different queries for multiple times and compute average ic */ 
		sim.multipleSimulation();
		/* plot probability on google map and IC vs Q */
		sim.plot(sim.isFeasible(), sim.plotICvsQuery(), "KCLUSTERING");
		/* send email */
		sim.sendEmail(sim.isFeasible(), sim.plotICvsQuery());
	}

}