import javatrix.*;
import java.util.*;

public class Interactive {
	public static void main(String[] args) throws Exception {
		Matrix base = generateMatrix();
		// printmatrix(base);
		while(true){
			Scanner inputReader = new Scanner(System.in);
			System.out.println("What do you want to do?");
			System.out.println("1. Change The Matrix");
			System.out.println("2. Print The Matrix");
			System.out.println("3. Get a section of the Matrix");
			System.out.println("4. Set a section of the Matrix");
			System.out.println("5. Transpose the Matrix");
			System.out.println("6. Find the InfNorm of the Matrix");
			System.out.println("7. Calculate the trace of the Matrix");
			System.out.println("8. Multiply the Matrix by a different Matrix");
			System.out.println("9. Divide the Matrix by a different Matrix, Element-wise");
			System.out.println("10. Exit the program");
			int option = inputReader.nextInt();
			switch(option){
				case 10:
					System.exit(0);
				case 1:
					base = generateMatrix();
					break;
				case 2:
					printmatrix(base);
					break;
				case 3:
					Matrix tmp = getmatrix(base, inputReader);
					System.out.println("Do you want to make this the new Matrix? 1 = Yes, 0 = No, 2 = Print it");
					int t = inputReader.nextInt();
					if(t == 1){
						base = tmp;
					}
					if(t == 2){
						printmatrix(tmp);
					}
					break;
				case 4:
					setmatrix(base, inputReader);
					break;
				case 5:
					Matrix trans = base.transpose();
					System.out.println("Do you want to make this the new Matrix? 1 = Yes, 0 = No, 2 = Print it");
					int p = inputReader.nextInt();
					if(p == 1){
						base = trans;
					}
					if(p == 2){
						printmatrix(trans);
					}
					break;
				case 6:
					System.out.println("The normInf of the matrix is:");
					System.out.println(base.normInf());
					break;
				case 7:
					System.out.println("The Trace of the matrix is:");
					System.out.println(base.trace());
					break;
				case 8:
					System.out.println("Generating Multiplication Matrix (Goes on the Right):");
					Matrix temp = generateMatrix();
					base = base.times(temp);
					break;
				case 9:
					System.out.println("Generating Division Matrix (Goes on the Right):");
					Matrix temq = generateMatrix();
					base = base.arrayRightDivide(temq);
					break;
			
			}
		
		}
	}

	public static void setmatrix(Matrix b, Scanner in) {
		System.out.println("What Row Indicies do you want to set? Please put all the indicies in one line, starting with the number of indicies");
		int times = in.nextInt();
		int[] rows = new int[times];
		for(int i = 0; i < times; i++){
			rows[i] = in.nextInt();
		}
		System.out.println("What Column Indicies do you want to set? Please put all the indicies in one line, starting with the number of indicies");
		times = in.nextInt();
		int[] cols = new int[times];
		for(int i = 0; i < times; i++){
			cols[i] = in.nextInt();
		}
		System.out.println("For the \"setting\" matrix:");
		Matrix newstuff = new Matrix(generatefrom2d(rows.length, cols.length, in));
		b.setMatrix(rows, cols, newstuff);
		

	}

	public static Matrix getmatrix(Matrix b, Scanner in) {
		System.out.println("What Row Indicies do you want? Please put all the indicies in one line, starting with the number of indicies");
		int times = in.nextInt();
		int[] rows = new int[times];
		for(int i = 0; i < times; i++){
			rows[i] = in.nextInt();
		}
		System.out.println("What Column Indicies do you want? Please put all the indicies in one line, starting with the number of indicies");
		times = in.nextInt();
		int[] cols = new int[times];
		for(int i = 0; i < times; i++){
			cols[i] = in.nextInt();
		}
		return b.getMatrix(rows, cols);
		
	}

	public static void printmatrix(Matrix b) {
		System.out.println("How do you want to print the matrix? Please input the width and the number of dps");
		Scanner inputReader = new Scanner(System.in);
		b.print(inputReader.nextInt(), inputReader.nextInt());
		
	}

	public static Matrix generateMatrix() throws Exception {
		int option;
		Scanner inputReader = new Scanner(System.in);
		System.out.println("What type of matrix do you want?");
		System.out.println("Please Specifiy dimensions, then select one of these:");
		System.out.println("1. Generated from 2D array");
		System.out.println("2. Generated from a scalar");
		System.out.println("3. Identity Matrix");
		System.out.println("Example input: 3 3 3, generates 3x3 identity matrix");
		int rows = inputReader.nextInt();
		int cols = inputReader.nextInt();
		option = inputReader.nextInt();

		switch(option){
			case 1:
				double[][] thing = generatefrom2d(rows, cols, inputReader);
				return new Matrix(thing);
			case 2:
				double scalar = inputReader.nextDouble();
				return new Matrix(rows, cols, scalar);
			case 3:
				return Matrix.identity(rows, cols);
			default:
				throw new Exception("No Option Selected");
		}
	}

	public static double[][] generatefrom2d(int rows, int cols, Scanner inputReader){
		double[][] answer = new double[rows][cols];
		System.out.println("Input data:");
		for(int i = 0; i < rows; i++){
			for(int j = 0; j < cols; j++){
				answer[i][j] = inputReader.nextDouble();
			}
		}
		return answer;
	}
}