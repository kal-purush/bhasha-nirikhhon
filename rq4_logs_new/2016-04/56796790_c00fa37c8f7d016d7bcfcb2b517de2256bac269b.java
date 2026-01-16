package com.mockito.test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Matchers.argThat;

import java.util.LinkedList;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class MockitoTestsDocumentation {

	@Test
	public void testList() {
		// mock creation
		List mockedList = mock(List.class);

		// using mock object
		mockedList.add("one");
		mockedList.clear();

		// verification
		verify(mockedList).add("one");
		verify(mockedList).clear();
	}

	@Test
	public void testSrubbing() {
		// You can mock concrete classes, not just interfaces
		LinkedList mockedList = mock(LinkedList.class);
		// stubbing
		when(mockedList.get(0)).thenReturn("first");
		when(mockedList.get(1)).thenReturn(new RuntimeException());
		// print first
		System.out.println(mockedList.get(0));
		// throws runtime exception
		System.out.println(mockedList.get(1));
		// null
		System.out.println(mockedList.get(2));
		// If your code doesn't care what get(0) returns, then it should not be
		// stubbed. Not convinced? See here.
		verify(mockedList).get(0);
	}

	@Test
	public void testArgumentMatch() {
		LinkedList mockedList = mock(LinkedList.class);
		// stubbing using built-in anyInt() ARGUMENT MATCHER
		when(mockedList.get(Mockito.anyInt())).thenReturn("any element 999");
		// stubbing using custom matcher (let's say isValid() returns your own
		// matcher implementation):
		when(mockedList.contains(argThat(isValid()))).thenReturn(false);

		System.out.println(mockedList.get(999));

		verify(mockedList).get(Mockito.anyInt());
	}

	private ArgumentMatcher<Boolean> isValid() {
		return new ArgumentMatcher<Boolean>() {
			@Override
			public boolean matches(Object argument) {
				return false;
			}

		};
	}

}