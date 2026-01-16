package ExceptionLesson1;

public class Main {
	public static void main(String[] args) {

		getArithmeticExeption();
		getNullPointerException();
		getIndexOfBoundException();

		int[] a = {1, 4, 2, 5, 7};
		int[] b = {3, 11, 4, 5, 5};
		try{
			for(int i : getSumm(a, b)){
				System.out.println(i);
			}

		} catch (RuntimeException e){
			System.out.println(e);
		}
		System.out.println();
		try{
			for(int i : getChastnoe(a, b)){
				System.out.println(i);
			}
		} catch (RuntimeException e){
			System.out.println(e);
		}
	}

	static void getArithmeticExeption(){
		int a = 10;
		int b = 0;
		try{
			int result = a/b;
		}
		catch (ArithmeticException e){
			System.out.println(e.fillInStackTrace());
		}

	}
	static void getNullPointerException(){
		String str =  null;
		try{
			System.out.println(str.length());
		}
		catch (NullPointerException e){
			System.out.println(e.fillInStackTrace());
		}
	}

	static void getIndexOfBoundException(){
		int[] arr = new int[5];
		try{
			System.out.println(arr[7]);
		}
		catch (IndexOutOfBoundsException e){
			System.out.println(e.fillInStackTrace());
		}
	}

	static int[] getSumm(int[] a, int[] b){
		int[] result = new int[a.length];
		if(a.length > b.length || b.length > a.length){
			throw new RuntimeException("Длины массивов не равны, суммирование невозможно");
		}
		for(int i = 0; i < a.length; i++){
			result[i] = a[i] - b[i];
		}
		return result;
	}
	static int[] getChastnoe(int[] a, int[] b){
		int[] result = new int[a.length];
		if(a.length > b.length || b.length > a.length){
			throw new RuntimeException("Длины массивов не равны, суммирование невозможно");
		}
		for(int i = 0; i < a.length; i++){
			result[i] = a[i] / b[i];
		}
		return result;
	}
}