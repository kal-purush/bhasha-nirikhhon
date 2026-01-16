
public class problem {

			public static void main(String[] args) {

				int N = 12;
				int brickCount = 0;
				int markBricks = 0;

				for(int johnBricks=1; johnBricks<N; johnBricks++){

					brickCount = brickCount + johnBricks;
					if(brickCount>=N){
						brickCount = brickCount - johnBricks;
						System.out.println("Wall Complete with "+N+" Bricks. John placed "+ (N-brickCount) + " brick(s) Lastly");
						break;
					}

					markBricks = johnBricks * 2;

					brickCount = brickCount + markBricks;

					if(brickCount>=N){
						brickCount = brickCount - markBricks;
						System.out.println("Wall Complete with "+N+" Bricks. Mark placed "+ (N-brickCount) + " brick(s) Lastly");
						break;

					}				}
				}
			}	
