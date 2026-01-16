package whiteSocket;

import java.awt.Color;

import whiteSocket.Blooprint;

public class Area {
	
	boolean[][] area = new boolean[Blooprint.sketch.getHeight()][Blooprint.sketch.getWidth()];
	int startX, startY;
	
	/*first border hit*/
	public static int borderStart_X,borderStart_Y;


	
	
	public void constructor(int x, int y) {
		this.startX = x;
		this.startY = y;
	}

	public static boolean[][] getSketchDrawnArea() throws Exception {
		/**
		 * returns binary map. area of interest on whiteboard, just outside of projected corners
		 * 
		 * TODO: need to scan for multiple getSketchDrawnArea areas.  so far we are only checking for
		 * the first one that we come across.
		 * */

		boolean[][] area = getUserDrawnBorder();

		int xStart = borderStart_X;
		int yStart = borderStart_Y + 2; /*TODO:	must consider the case in which borderStart_Y+2 is not inside border wall*/

		area = Blooprint.floodBorder(area, xStart,yStart);

		return area;
	}//END getSketchDrawnArea()

	public static boolean[][] getUserDrawnBorder() {
		/**
		 * dealing with area drawn by user to erase
		 * sets binary map single pixel strand border for future use in floodBorder() method
		
		TODO:
		after first border pixel is hit, continue searching through rest of sketch image for other areas
		*/

		boolean[][] border = new boolean[Blooprint.sketch.getHeight()][Blooprint.sketch.getWidth()];

		here:

			for(int row = 0; row < Blooprint.sketch.getHeight(); row++){
				for(int col = 0; col < Blooprint.sketch.getWidth(); col++){

					/*
					 * dealing with pixels input by user - sketch
					 * */
					Color pxColor = new Color(Blooprint.sketch.getRGB(col,row));
					int xIN = col;
		            int yIN = row;

		            if(Blooprint.areaOfInterest[row][col] && Blooprint.isMarker(pxColor)){

						System.out.println("found eraser border!!!");

						/*
						 * encapsulate eraser area
						 * */
						int[] inCoord = new int[2];
						inCoord[0] = col;
						inCoord[1] = row;
						boolean flag = true;
						while(flag){

							//	2dArray[y][x]
							border[inCoord[1]][inCoord[0]] = true;

							inCoord = getNextBorderPixel(inCoord);

							if((inCoord[0] == xIN) && (inCoord[1] == yIN)){

								borderStart_X = xIN;
								borderStart_Y = yIN;

								System.out.println("made it all the way around the border");

								flag = false;
								break here;
							}
						}
					}
					else{
						continue;
					}
				}
			}//END outer loop


		return border;
	}//END getUserDrawnBorder()

	/**
	 * recursive method locating pixel after last found pixel until entire border is lined
	 * */
	public static int[] getNextBorderPixel(int[] coord) {

		int[] next = new int[2];

		/*
		 * all 8 surrounding pixels need to be checked counterclockwise
		 * */
		Color a = new Color(Blooprint.sketch.getRGB(coord[0]+1,coord[1]));	//	R
		Color b = new Color(Blooprint.sketch.getRGB(coord[0]+1,coord[1]+1));	//	RD
		Color c = new Color(Blooprint.sketch.getRGB(coord[0],coord[1]+1));	//	D
		Color d = new Color(Blooprint.sketch.getRGB(coord[0]-1,coord[1]+1));	//	LD
		Color e = new Color(Blooprint.sketch.getRGB(coord[0]-1,coord[1]));	//	L
		Color f = new Color(Blooprint.sketch.getRGB(coord[0]-1,coord[1]-1));	//	LU
		Color g = new Color(Blooprint.sketch.getRGB(coord[0],coord[1]-1));	//	U
		Color h = new Color(Blooprint.sketch.getRGB(coord[0]+1,coord[1]-1));	//	RU


		if(Blooprint.isMarker(a) & !Blooprint.isMarker(h)){
			next[0] = coord[0]+1;	//	R
			next[1] = coord[1];
		}
		else if(Blooprint.isMarker(b) & !Blooprint.isMarker(a)){
			next[0] = coord[0]+1;	//	RD
			next[1] = coord[1]+1;
		}
		else if(Blooprint.isMarker(c) & !Blooprint.isMarker(b)){
			next[0] = coord[0];		//	D
			next[1] = coord[1]+1;
		}
		else if(Blooprint.isMarker(d) & !Blooprint.isMarker(c)){
			next[0] = coord[0]-1;	//	LD
			next[1] = coord[1]+1;
		}
		else if(Blooprint.isMarker(e) & !Blooprint.isMarker(d)){
			next[0] = coord[0]-1;	//	L
			next[1] = coord[1];
		}
		else if(Blooprint.isMarker(f) & !Blooprint.isMarker(e)){
			next[0] = coord[0]-1;	//	LU
			next[1] = coord[1]-1;
		}
		else if(Blooprint.isMarker(g) & !Blooprint.isMarker(f)){
			next[0] = coord[0];		//	U
			next[1] = coord[1]-1;
		}
		else if(Blooprint.isMarker(h) & !Blooprint.isMarker(g)){
			next[0] = coord[0]+1;	//	RU
			next[1] = coord[1]-1;
		}
		else{
			System.err.println("something wrong with setting next border pixel!!!");
		}


		return next;
	}//END getNextBorderPixel()



}