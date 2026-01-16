package be.ugent.psb.setas.bdmodel.model;

public class CacheProbs {

	public double [][][][] cacheAllProbs(int maxS, int maxC, double [] bLens, double [] lambdas){
		
		
		double [][][][] prb = new double [maxS+1][maxC+1][bLens.length][lambdas.length];
		
		
		ProbCalculator probCalc = new ProbCalculator();
		for(int s=1; s< maxS; s++){
			
//			System.out.println("s "+s);
			
			for(int c=1; c< maxC; c++){
//				System.out.println("c "+c);
				
				for(int t=0; t<bLens.length;t++){
					
//					System.out.println("t "+t);
					
					for(int lam=0; lam<lambdas.length;lam++){
						
						prb[s][c][t][lam]= probCalc.probCalc(lambdas[lam], bLens[t], s, c);
						
					}
				}
				
			}
			
		}
		
		return prb;
		
	}
	
}