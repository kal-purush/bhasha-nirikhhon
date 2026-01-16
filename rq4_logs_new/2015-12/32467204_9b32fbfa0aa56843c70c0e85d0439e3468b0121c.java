package extra;

import java.util.ArrayList;

import CA.CAIBASM;

public class extraReplicationSMIBA {
	final static byte[][][] kandidates = new byte[][][]{
		{	{12,0,1,1,8,},{17,1,4,3,4,},{17,2,1,8,3,},{9,1,3,9,3,},{1,3,1,5,0,},{15,0,3,8,3,},{16,4,1,7,3,},{14,4,4,5,1,},{2,1,4,6,8,},{3,0,1,7,1,},	},
		{	{17,2,2,2,3,},{17,0,4,6,3,},{20,3,4,4,9,},{9,0,3,0,0,},{9,4,4,6,0,},{2,0,1,0,8,},{13,3,3,7,1,},{9,0,0,5,0,},{21,1,3,1,2,},{12,1,4,0,8,},	},
		{	{14,3,3,3,2,},{14,3,4,8,3,},{20,1,0,0,6,},{2,4,0,8,4,},{11,3,3,5,5,},{13,4,1,5,6,},{17,0,0,7,5,},{12,3,2,7,3,},{15,1,2,3,2,},{9,1,2,4,3,},	},
		{	{9,0,1,8,4,},{17,4,3,7,4,},{2,2,3,6,8,},{20,2,3,8,7,},{13,1,0,6,9,},{3,3,1,0,8,},{11,3,0,4,7,},{2,4,0,6,3,},{5,4,2,6,7,},{8,0,4,1,8,},	},
		{	{17,1,2,4,2,},{13,4,3,0,9,},{13,3,2,7,6,},{11,3,1,9,6,},{2,3,2,3,7,},{14,4,0,0,9,},{9,3,4,7,6,},{14,1,3,0,4,},{5,2,2,5,6,},{16,3,2,2,1,},	},
		{	{9,3,4,8,4,},{9,4,3,7,0,},{3,2,3,6,9,},{17,1,3,6,8,},{9,0,1,1,6,},{2,0,3,0,8,},{13,4,3,5,3,},{2,4,3,1,5,},{11,3,0,3,6,},{1,2,3,3,6,},	},
		{	{0,0,3,9,4,},{18,1,4,7,3,},{2,3,1,6,5,},{9,4,3,4,1,},{8,0,2,4,9,},{5,0,3,1,8,},{7,1,0,5,5,},{13,4,0,1,2,},{1,0,4,5,8,},{0,3,1,4,4,},	},
		{	{2,4,0,9,3,},{10,3,2,8,6,},{13,4,3,6,0,},{11,1,4,4,9,},{9,4,4,7,8,},{9,0,3,3,2,},{17,0,0,2,0,},{2,0,1,2,3,},{14,0,3,7,2,},{7,1,3,1,1,},	},
		{	{13,3,3,3,4,},{14,3,0,9,3,},{13,4,2,1,8,},{7,4,2,9,9,},{17,3,3,0,5,},{9,0,2,9,1,},{9,1,1,4,4,},{15,0,0,3,4,},{2,1,0,4,3,},{3,1,3,1,6,},	},
		{	{14,1,2,2,2,},{17,4,2,0,3,},{2,2,1,1,4,},{9,0,2,7,3,},{11,4,4,3,6,},{4,4,4,8,7,},{17,4,4,9,5,},{11,3,0,3,8,},{8,4,0,5,7,},{13,3,2,1,2,},	},
		{	{15,4,4,0,5,},{2,1,0,6,8,},{17,0,4,8,6,},{3,4,2,4,1,},{14,4,2,8,7,},{18,4,1,2,0,},{9,1,0,0,6,},{17,1,1,2,4,},{2,3,1,3,2,},{5,4,0,3,4,},	},
		{	{17,3,1,9,9,},{6,1,1,8,7,},{3,0,1,0,6,},{13,1,2,1,2,},{2,2,1,3,9,},{7,1,2,4,3,},{17,0,3,7,5,},{14,0,0,6,7,},{17,2,4,2,4,},{9,1,0,7,9,},	},
		{	{2,0,3,2,3,},{17,2,0,6,1,},{13,2,2,0,6,},{16,0,3,6,9,},{11,1,1,4,9,},{9,3,0,1,7,},{10,0,1,6,4,},{18,4,2,4,0,},{14,4,0,0,4,},{17,3,4,9,8,},	},
		{	{9,3,3,9,3,},{13,0,1,6,2,},{12,2,1,6,5,},{2,3,2,0,4,},{12,0,4,9,4,},{5,0,4,5,0,},{7,0,2,8,9,},{17,4,0,0,3,},{13,2,0,5,9,},{21,0,4,5,5,},	},
		{	{2,0,2,3,8,},{2,1,3,9,3,},{16,2,0,4,6,},{11,0,1,0,0,},{5,2,1,0,8,},{13,4,2,6,1,},{10,3,4,6,0,},{9,3,0,6,9,},{9,1,4,5,5,},{17,0,0,5,3,},	},
		{	{17,1,1,7,1,},{9,3,0,0,9,},{14,1,0,6,4,},{10,4,2,8,9,},{3,1,2,0,9,},{2,4,0,0,1,},{14,3,0,8,7,},{9,4,3,6,4,},{4,2,3,7,1,},{0,2,0,7,7,},	},
		{	{15,4,4,3,8,},{17,0,1,6,3,},{18,1,3,9,8,},{4,1,4,8,9,},{3,1,3,2,8,},{14,1,0,0,0,},{2,0,3,7,8,},{9,0,0,9,9,},{11,1,3,2,4,},{13,2,3,3,7,},	},
		{	{13,3,4,1,0,},{12,0,4,1,5,},{17,1,1,4,0,},{2,3,1,5,7,},{9,3,2,5,6,},{7,1,4,7,8,},{13,0,2,1,4,},{20,0,1,5,3,},{3,4,4,2,1,},{19,0,0,9,3,},	},
		{	{17,3,4,1,4,},{12,1,2,4,2,},{1,0,4,1,3,},{14,0,2,9,1,},{12,4,4,1,3,},{1,2,2,2,8,},{9,4,0,2,0,},{13,3,1,3,7,},{17,0,4,6,9,},{2,4,3,6,7,},	},
		{	{9,1,3,8,7,},{1,2,3,8,8,},{2,2,1,4,2,},{9,3,1,3,1,},{8,4,4,7,8,},{13,1,0,8,9,},{7,1,4,4,4,},{3,2,3,7,1,},{5,4,2,0,8,},{13,0,0,7,8,},	},
		{	{10,0,3,9,7,},{13,3,4,9,4,},{17,0,3,9,4,},{7,1,1,1,4,},{9,3,0,9,2,},{2,0,2,4,9,},{11,4,4,4,5,},{17,4,4,8,3,},{14,0,3,8,1,},{2,3,2,6,6,},	},
		{	{9,2,1,6,7,},{1,1,1,2,6,},{13,4,2,2,1,},{21,0,0,4,9,},{12,4,0,4,7,},{17,0,3,0,6,},{2,2,3,3,5,},{7,3,1,9,6,},{17,4,4,2,0,},{17,2,2,7,9,},	},


	};
	
	CAIBASM generator; 
	int [][] target;
	int boardSize;
	int [][] freshBoard;
	int maxDevIterations;

	public extraReplicationSMIBA(int maxDevIterations, int dimentions, int boardSize,
			int numOfStates, boolean elitism, int numberOfInstructions){
		generator = new CAIBASM(dimentions, boardSize, numOfStates, false, numberOfInstructions);
		this.maxDevIterations = maxDevIterations;
		this.boardSize = boardSize;
	}
	final static byte[][][] test = new byte[][][]{

	};
	
	public void setTarget(int [][] target){
		int [][] dot = new int[1][1];
		dot[0][0] = 1;
		this.target = target;
		generateReplicationBoard(target);
	//	this.maxFitness = target.length*target[0].length /*numReplicated/*2/**/;
	}
	private void generateReplicationBoard(int [][] replicator){
		int x = (boardSize/2) - (replicator.length/2);
		int y = (boardSize/2) - (replicator[0].length /2);

		int [][] newBoard = new int [boardSize][boardSize];
		for (int i = x; i < x+replicator.length; i++) {
			for (int j =y; j < y+replicator[0].length; j++) {
				newBoard[i][j] = replicator[i-x][j-y];
			}
		}
		freshBoard = newBoard;

	}

	private double replicationFitnessFunction(byte[][] rules, int nrReplicated){
		int partielFitness = 0;
		int fitness= 0;
		int maxFitness = 0;
		int [] bestPartials = new int [nrReplicated];
		generator.setBoard(freshBoard);
		generator.setRules(rules);
		generator.start(5);
		for (int k = 0; k < (maxDevIterations-5); k++) {

			generator.start(1);
			int board[][] = generator.getBoard();
			bestPartials = new int [nrReplicated];
			for (int i = 0; i < board.length - target.length; i++) {
				for (int j = 0; j < board[0].length - target[0].length; j++) {
					partielFitness = 0;
					for (int j2 = 0; j2 < target.length; j2++) {
						for (int l = 0; l < target[0].length; l++) {
							if(board[(i+j2)][(j+l)]==target[j2][l]){
								partielFitness++;
							}
						}
					}
					for (int j2 = 0; j2 < nrReplicated; j2++) {
						if(bestPartials[j2]< partielFitness){
							bestPartials[j2] = partielFitness;
							break;
						}
					}
				}
			}
			fitness = 0;
			for (int i = 0; i < nrReplicated; i++) {
				if(bestPartials[i]==target.length*target[0].length)
				fitness += 1;

			}
			if(fitness>maxFitness){
				maxFitness=fitness;
			}


		}

		return maxFitness;
	}
	
	public static void main(String[] args) {
		final int[][] simpleStructureBorderd = new int[][]{
				{0,0,0,0,0,0,0},
				{0,1,1,1,1,1,0},
				{0,1,0,1,0,1,0},
				{0,1,1,1,1,1,0},
				{0,1,0,1,0,1,0},
				{0,1,1,1,1,1,0},
				{0,0,0,0,0,0,0},


		};
		final int[][] frenchFlagBorderd = new int[][]{
				{0,0,0,0,0,0,0,0},
				{0,1,1,0,0,2,2,0},
				{0,1,1,0,0,2,2,0},
				{0,1,1,0,0,2,2,0},
				{0,1,1,0,0,2,2,0},
				{0,1,1,0,0,2,2,0},
				{0,1,1,0,0,2,2,0},
				{0,0,0,0,0,0,0,0},


		};
		
		final int[][] flagBorderd = new int[][]{
				{0,0,0,0,0,0,0,0},
				{0,1,1,2,2,3,3,0},
				{0,1,1,2,2,3,3,0},
				{0,1,1,2,2,3,3,0},
				{0,1,1,2,2,3,3,0},
				{0,1,1,2,2,3,3,0},
				{0,1,1,2,2,3,3,0},
				{0,0,0,0,0,0,0,0},


		};
		final int[][] norFlagBorderd = new int[][]{
				{0,0,0,0,0,0,0,0},
				{0,2,0,1,0,2,2,0},
				{0,0,0,1,0,0,0,0},
				{0,1,1,1,1,1,1,0},
				{0,0,0,1,0,0,0,0},
				{0,2,0,1,0,2,2,0},
				{0,2,0,1,0,2,2,0},
				{0,0,0,0,0,0,0,0},


		};
		final int[][] creeperRep = new int[][]{
				{2,2,2,2,2,2,2,2},
				{2,1,1,2,2,1,1,2},
				{2,1,1,2,2,1,1,2},
				{2,2,2,1,1,2,2,2},
				{2,2,1,1,1,1,2,2},
				{2,2,1,1,1,1,2,2},
				{2,2,1,2,2,1,2,2},
				{2,2,2,2,2,2,2,2},
		};
		extraReplicationSMIBA tester = new extraReplicationSMIBA(120, 2, 75, 3, true, 10);
		tester.setTarget(creeperRep);
		ArrayList<Double> values = new ArrayList<Double>();
		double sum = 0;
		double best = 0, bestNr;
		double intermediat = 0;
		for (int i = 0; i < kandidates.length; i++) {
			intermediat = tester.replicationFitnessFunction(kandidates[i], 100);
			values.add (intermediat);
			sum+=intermediat;
			if (best < intermediat) {
				bestNr = i;
				best = intermediat;
				System.out.println(bestNr + ":" +best);
			}
		}
		
		System.out.println("Average: " + sum/values.size());
		
		//stdev
		System.out.println("StDev: " + stDev(values));
	}
	
	private static double stDev(ArrayList<Double> set){
		double sum=0, sumMeanSquared = 0;
		double avarage;
		
		for (Double integer : set) {
			sum+=integer;
		}
		avarage = sum/set.size();
		for (Double integer : set) {
			sumMeanSquared += Math.pow((integer-avarage),2);
			//System.out.println(Math.pow((integer.doubleValue()-avarage),2));
		}
		//System.out.println(sumMeanSquared);
		
		return Math.sqrt(sumMeanSquared/set.size());
	}
	

}