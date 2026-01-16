package com.github.eduardovalentim.easymath;

import static java.util.Arrays.asList;

import java.math.BigInteger;
import java.math.MathContext;
import java.util.Collection;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.github.eduardovalentim.easymath.functions.FactorialFunction;
import com.github.eduardovalentim.easymath.functions.PowerFunction;

class FunctionCatalogTest {

	@Test
	void testFunctionCatalogDefaultConstructor() {
		FunctionCatalog catalog = new FunctionCatalog();
		Assertions.assertNotNull(catalog);
	}

	@Test
	void testFunctionCatalogArrayConstructor() {
		FunctionCatalog catalog = new FunctionCatalog("test", FactorialFunction.INSTANCE);
		Assertions.assertNotNull(catalog);
	}

	@Test
	void testFunctionCatalogArrayConstructorNameNull() {
		Assertions.assertThrows(IllegalArgumentException.class, () -> {
			new FunctionCatalog(null, FactorialFunction.INSTANCE);
		});
	}

	@Test
	void testFunctionCatalogArrayConstructorNameEmpty() {
		Assertions.assertThrows(IllegalArgumentException.class, () -> {
			new FunctionCatalog("", FactorialFunction.INSTANCE);
		});
	}

	@Test
	void testFunctionCatalogArrayConstructorFunctionsNull() {
		Assertions.assertThrows(IllegalArgumentException.class, () -> {
			Function<?>[] functions = null;
			new FunctionCatalog("test", functions);
		});
	}

	@Test
	void testFunctionCatalogArrayConstructorFunctionNull() {
		Assertions.assertThrows(IllegalArgumentException.class, () -> {
			Function<?>[] functions = { null };
			new FunctionCatalog("test", functions);
		});
	}

	@Test
	void testFunctionCatalogCollectionConstructor() {
		FunctionCatalog catalog = new FunctionCatalog("test", asList(FactorialFunction.INSTANCE));
		Assertions.assertNotNull(catalog);
	}

	@Test
	void testFunctionCatalogCollectionConstructorNameNull() {
		List<Function<?>> functions = asList(FactorialFunction.INSTANCE);
		Assertions.assertThrows(IllegalArgumentException.class, () -> {
			new FunctionCatalog(null, functions);
		});
	}

	@Test
	void testFunctionCatalogCollectionConstructorNameEmpty() {
		Collection<Function<?>> functions = asList(FactorialFunction.INSTANCE);
		Assertions.assertThrows(IllegalArgumentException.class, () -> {
			new FunctionCatalog("", functions);
		});
	}

	@Test
	void testFunctionCatalogCollectionConstructorFunctionsNull() {
		Assertions.assertThrows(IllegalArgumentException.class, () -> {
			Collection<Function<?>> functions = null;
			new FunctionCatalog("test", functions);
		});
	}

	@Test
	void testFunctionCatalogCollectionConstructorFunctionNull() {
		Function<?> function = null;
		Collection<Function<?>> functions = asList(function);
		Assertions.assertThrows(IllegalArgumentException.class, () -> {
			new FunctionCatalog("test", functions);
		});
	}

	@Test
	void testAddFunctionWithNameNull() {
		FunctionCatalog catalog = new FunctionCatalog("test", FactorialFunction.INSTANCE);
		Function<?> function = Mockito.mock(Function.class);
		Mockito.when(function.name()).thenReturn(null);
		Assertions.assertThrows(IllegalArgumentException.class, () -> {
			catalog.addFunction(function);
		});
	}

	@Test
	void testAddFunctionWithNameEmpty() {
		FunctionCatalog catalog = new FunctionCatalog("test", FactorialFunction.INSTANCE);
		Function<?> function = Mockito.mock(Function.class);
		Mockito.when(function.name()).thenReturn("");
		Assertions.assertThrows(IllegalArgumentException.class, () -> {
			catalog.addFunction(function);
		});
	}

	@Test
	void testAddAllFunctions() {
		FunctionCatalog catalog = new FunctionCatalog("test", FactorialFunction.INSTANCE);
		catalog.addAllFunctions(asList(FactorialFunction.INSTANCE));
		Assertions.assertNotNull(catalog);
	}

	@Test
	void testAddAllFunctionsWithNull() {
		FunctionCatalog catalog = new FunctionCatalog("test", FactorialFunction.INSTANCE);
		Assertions.assertThrows(IllegalArgumentException.class, () -> {
			catalog.addAllFunctions(null);
		});
	}

	@Test
	void testJoinCatalog() {
		FunctionCatalog catalogA = new FunctionCatalog("a", FactorialFunction.INSTANCE);
		FunctionCatalog catalogB = new FunctionCatalog("b", PowerFunction.INSTANCE);

		Assertions.assertNotNull(catalogA.join(catalogB));
	}

	@Test
	void testJoinCatalogWithNull() {
		FunctionCatalog catalog = new FunctionCatalog("test", FactorialFunction.INSTANCE);
		FunctionCatalog[] catalogs = null;
		Assertions.assertThrows(IllegalArgumentException.class, () -> {
			catalog.join(catalogs);
		});
	}

	@Test
	void testJoinCatalogWithNullElement() {
		FunctionCatalog catalog = new FunctionCatalog("test", FactorialFunction.INSTANCE);
		FunctionCatalog[] catalogs = { null };
		Assertions.assertThrows(IllegalArgumentException.class, () -> {
			catalog.join(catalogs);
		});
	}

	@Test
	void testSolve() {
		FunctionCatalog catalog = new FunctionCatalog("test", FactorialFunction.INSTANCE);
		BigInteger actual = catalog.solve("fat", MathContext.DECIMAL32, 5d);
		Assertions.assertEquals(new BigInteger("120"), actual);
	}

	@Test
	void testSolveWithNameNull() {
		FunctionCatalog catalog = new FunctionCatalog("test", FactorialFunction.INSTANCE);
		Assertions.assertThrows(IllegalArgumentException.class, () -> {
			catalog.solve(null, MathContext.DECIMAL32, 5d);
		});
	}

	@Test
	void testSolveWithNameEmpty() {
		FunctionCatalog catalog = new FunctionCatalog("test", FactorialFunction.INSTANCE);
		Assertions.assertThrows(IllegalArgumentException.class, () -> {
			catalog.solve("", MathContext.DECIMAL32, 5d);
		});
	}

	@Test
	void testSolveWithInputNull() {
		FunctionCatalog catalog = new FunctionCatalog("test", FactorialFunction.INSTANCE);
		Number[] inputs = null;
		Assertions.assertThrows(IllegalArgumentException.class, () -> {
			catalog.solve("fat", MathContext.DECIMAL32, inputs);
		});
	}

	@Test
	void testSolveWithFunctionNull() {
		FunctionCatalog catalog = new FunctionCatalog("test", FactorialFunction.INSTANCE);
		Assertions.assertThrows(IllegalStateException.class, () -> {
			catalog.solve("fatorial", MathContext.DECIMAL32, 5.0d);
		});
	}
	
	@Test
	void testValueOf() {
		FunctionCatalog catalog = FunctionCatalog.valueOf("test", FactorialFunction.INSTANCE);
		Assertions.assertNotNull(catalog);
	}

	@Test
	void testGetName() {
		FunctionCatalog catalog = FunctionCatalog.valueOf("test", FactorialFunction.INSTANCE);
		Assertions.assertEquals("test", catalog.getName());
	}
}