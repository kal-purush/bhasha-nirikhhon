package inf2;
import java.util.Scanner;
public class matrix {

	public static void main(String[] args) {
		//Scanner imp=new Scanner(System.in);
//int m=imp.nextInt();
//int n= imp.nextInt();

		//int [][] matrix = new int [n][m];//broi elementi i vuvejdane
		//int i;
		//int j;
		
		//obhojdane
		//for (i=0;i<4/*n*/;i++) {
		//	for (j=0;j<4/*m*/;j++)
				
		//matrix[i][j]=imp.nextInt();//vuvejdane el na masiva
		
			//System.out.print(matrix[i][j]+" ");//izvejdane
			//} 
		Scanner sc=new Scanner(System.in);
		//<тип на матр.>[][] <име на матр.>=new <тип>[бр редове][бр колони];
		        int n=sc.nextInt(); int m=sc.nextInt();
		        int[][] matrix=new int[n][m];

		 

		        //въвеждане на елементите на масива
		       for (int i=0;i<n;i++){
		           for(int j=0;j<m;j++)
		                matrix[i][j]=sc.nextInt();
		        }
		        //извеждане на елементите на масива
		        for (int i=0;i<n;i++){
		        System.out.println();
		            for(int j=0;j<m;j++)
		                System.out.print(matrix[i][j]+" ");
		        } 
		      int p=0;
		        // да намерин сбора на е-те по главен дагонал
		        for (int i=0;i<n;i++){
		            for(int j=0;j<m;j++)
		           if (i==j) p+=matrix[i][j];
		               
		           } 
		       System.out.println("sbora e ="+p);
		        
		        
		        
		}
	}

