package ru.aston.tarakanov_aa.task6;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.Arrays;

public class StreamApi {	

	public static void main(String[] args) {
		
//		1. Соберите числа в Stream в одно число, перемножив их между собой и выведите результат.
		int product = Stream.of(1, 2, 3, 4, 5).reduce(1, (a, b) -> a * b);
		System.out.println(product);	
		
		
//		2. Задан массив строк. Используя средства StreamAPI отсортировать строки в лексикографическом порядке.
		List<String> sortedList = Stream.of("Nastya", "Vika", "Masha", "Anna", "Olga").sorted((o1, o2) -> o1.compareTo(o2)).toList();
		System.out.println(sortedList);
		
		
//		3. Создайте Stream из массива чисел, выведите на экран числа, кратные 3 и 5 одновременно.
		int[] intArray = {1, 3, 5, 15, 17, 20, 30, 45, 60, 65};
		List<Integer> intList = Arrays.stream(intArray).filter(x -> x % 3 == 0).filter(x -> x % 5 == 0).boxed().toList();
		System.out.println(intList);
		
	
//		4. Отфильтруйте все элементы списка, которые меньше 0.
		List<Integer> positiveNumbers = Stream.of(-1, 2, 4, 10, -10, -7, 8).filter(x -> x > 0).toList();
		System.out.println(positiveNumbers);		
		
				
//		5. Создание трех Stream из массивов и объединение их в один. Затем вывод на экран среднего геометрического значения элементов этого Stream.
		int[] intArray1 = {1, 3, 5, 15};
		int[] intArray2 = {2, 4, 7, 18};
		int[] intArray3 = {3, 4, 9, 11};
		int cntArrays = intArray1.length + intArray1.length + intArray3.length;
		int productArrays = Stream.of(Arrays.stream(intArray1), Arrays.stream(intArray2), Arrays.stream(intArray3))
																				.flatMap(x -> x.boxed()).reduce(1, (a, b) -> a*b);
		double geoMean = Math.pow(productArrays, 1.0 / cntArrays);
		System.out.println(geoMean);	
		
		
//		6. Создание Stream из массива целых чисел и вывод на экран всех простых чисел в диапазоне от 2 до 100.
		List<Integer> primeList = IntStream.rangeClosed(2, 100).filter(x -> {
			for(int i=2; i<=Math.sqrt(x); i++) {
				if (x % i == 0) {
					return false;
				}
			}
			return true;
		}).boxed().toList();
		System.out.println(primeList);
		
		
//		7. Разделить элементы Stream на две группы: четные и нечетные, вывести результаты.
		Map<String, List<Integer>> mapNumbers = IntStream.rangeClosed(1, 30).boxed().collect(Collectors.groupingBy(x -> {
			if (x % 2 == 0) {
				return "Even numbers";
			}
			return "Not even numbers";
		}));
		System.out.println(mapNumbers);
		
		
//		8. Создать стрим четных чисел от 2 до 40 и вывести на экран количество элементов в этом стриме.
		long count = IntStream.range(2, 41).count();
		System.out.println(count);
		
		
//		9. Создать два стрима: один из чисел от 0 до 10, другой из чисел от 10 до 20. Объединить их в один стрим и вывести на экран числа больше 10.
		IntStream stream1 = IntStream.rangeClosed(0, 10);
		IntStream stream2 = IntStream.rangeClosed(10, 20);
		List<Integer> mergeStreams = IntStream.concat(stream1, stream2).filter(x -> x > 10).boxed().toList();
		System.out.println(mergeStreams);
		
		
//		10. Создайте Stream чисел от 2 до 10. Умножьте их на 2 и выведите результат на экран, ограничьтесь первыми десятью результатами.
		List<Integer> limitStream = IntStream.rangeClosed(0, 10).map(x -> x*2).limit(10).boxed().toList();
		System.out.println(limitStream);
		
	}
}