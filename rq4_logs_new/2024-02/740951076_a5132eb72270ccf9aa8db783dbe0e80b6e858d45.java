package arrayAufgaben;

public class Aufgabe_4 {
	
	
	
	public static int[][] magnify(int[][] array, int factor) {
	    if (factor <= 0) {
	        throw new IllegalArgumentException("Der Faktor muss gr��er als 0 sein.");
	    }
	 
	    int Height = array.length;
	    int Width = array[0].length;
	    int newHeight = Height * factor;
	    int newWidth = Width * factor;
	    int[][] magnifiedImage = new int[newHeight][newWidth];
	 
	    for (int i = 0; i < newHeight; i++) {
	    	 for (int j = 0; j < newWidth; j++) {
	    	        magnifiedImage[i][j] = array[i / factor][j / factor];
	    	    }
	    	}
	 
	    	return magnifiedImage;
	    }
	
	
	
	public static void main(String[] args) {
		int[][] Image = {
		        {1, 2, 3},
		        {4, 5, 6}
		    };
			int factor = 6;
		    int[][] Images = magnify(Image, factor);
		    for (int i = 0; i < Images.length; i++) {
		        for (int j = 0; j < Images[i].length; j++) {
		            System.out.print(Images[i][j] + " ");
		        }
		        System.out.println();
		    }
	}
}