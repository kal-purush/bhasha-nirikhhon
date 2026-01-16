package math.function.linear;

import java.util.ArrayList;
import java.util.List;

import util.TriFunction;

/**
 * @author Paweł Głogowski
 */
public class LinearFunction {
	public static <A, B, C, R> List<R> calculateSolutions(A a, B b, TriFunction<A, B, C, R> function, List<C> pointsX) {
		List<R> result = new ArrayList<>();

		pointsX.forEach(x -> result.add(function.apply(a, b, x)));

		return result;
	}
}