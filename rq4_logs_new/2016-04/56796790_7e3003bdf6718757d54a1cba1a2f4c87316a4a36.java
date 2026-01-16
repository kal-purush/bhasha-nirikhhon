package com.mockito.test;

import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.inOrder;

import java.util.Calendar;
import java.util.Date;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.exceptions.base.MockitoException;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.runners.MockitoJUnitRunner;
import org.mockito.stubbing.Answer;

import testClases.Flower;
import testClases.FlowerFilter;
import testClases.FlowerSearcher;
import testClases.PlantSearcher;
import testClases.SearchCriteria;
import testClases.WaterException;
import testClases.WaterSource;
import testClases.WateringScheduler;

@RunWith(MockitoJUnitRunner.class)
public class SimpleStubbingTest {

	static {
		Calendar cal = Calendar.getInstance();
		WANTED_DATE = cal.getTime();
		cal.add(Calendar.HOUR, 1);
		ANY_OTHER_DATE = cal.getTime();
	}

	public static final int TEST_NUMBER_OF_LEAFS = 5;
	public static final Date WANTED_DATE;
	public static final int VALUE_FOR_WANTED_ARGUMENT = 500;
	public static final Date ANY_OTHER_DATE;
	public static final int TEST_NUMBER_OF_FLOWERS = 77;

	@Mock
	private Flower flowerMock;

	@Mock
	private WateringScheduler wateringSchedulerMock;

	@Mock
	private PlantSearcher plantSearcherMock;

	@Mock
	private WaterSource waterSourceMock;

	@Mock
	private FlowerFilter flowerFilterMock;

	@Mock
	private FlowerSearcher flowerSearcherMock;

	@Test
	public void shouldReturnGivenValue() {
		// (given) SUT and mocks initialization and configuration
		when(flowerMock.getNumberOfLeafs()).thenReturn(TEST_NUMBER_OF_LEAFS);

		// (when) An operation which is a subject to testing; preferably only
		// one operation on an SUT
		int numberOfLeafs = flowerMock.getNumberOfLeafs();

		// (then) The assertion and verification phase
		assertEquals(numberOfLeafs, TEST_NUMBER_OF_LEAFS);

	}

	/**
	 * UsingBDDSemantics
	 */
	@Test
	public void shouldReturnGivenValueUsingBDDSemantics() {
		// given
		given(flowerMock.getNumberOfLeafs()).willReturn(TEST_NUMBER_OF_LEAFS);
		// when
		int numberOfLeafs = flowerMock.getNumberOfLeafs();
		// then
		assertEquals(numberOfLeafs, TEST_NUMBER_OF_LEAFS);
	}

	@Test
	public void shouldMatchSimpleArgument() {
		given(wateringSchedulerMock.getNumberOfPlantsScheduledOnDate(WANTED_DATE))
				.willReturn(VALUE_FOR_WANTED_ARGUMENT);

		int numberForWantedArgument = wateringSchedulerMock.getNumberOfPlantsScheduledOnDate(WANTED_DATE);
		int numberForAnyOtherArgument = wateringSchedulerMock.getNumberOfPlantsScheduledOnDate(ANY_OTHER_DATE);

		assertEquals(numberForWantedArgument, VALUE_FOR_WANTED_ARGUMENT);
		assertEquals(numberForAnyOtherArgument, 0);

	}

	@Test
	public void matchers() {
		// If an argument matcher is used for at least one argument, all
		// arguments must be provided by matchers.
		given(plantSearcherMock.smellyMethod(Mockito.anyInt(), Mockito.contains("asparag"), Mockito.eq("red")))
				.willReturn(true);

	}

	@Test
	public void extendingTheArgumentMatcher() {
		given(wateringSchedulerMock.getNumberOfPlantsScheduledOnDate(Mockito.argThat(haveHourFieldEqualTo(123))))
				.willReturn(11111);

	}

	private ArgumentMatcher<Date> haveHourFieldEqualTo(final int hour) {
		return new ArgumentMatcher<Date>() {
			@Override
			public boolean matches(Object argument) {
				return ((Date) argument).getHours() == hour;
			}
		};
	}

	@Test
	public void shouldReturnLastDefinedValueConsistently() {
		given(waterSourceMock.getWaterPressure()).willReturn(3, 5);

		assertEquals(waterSourceMock.getWaterPressure(), 3);
		assertEquals(waterSourceMock.getWaterPressure(), 5);
		assertEquals(waterSourceMock.getWaterPressure(), 5);
		// truena
		// assertEquals(waterSourceMock.getWaterPressure(), 3);
	}

	/**
	 * Void methods
	 */
	// @Test(expectedExceptions = WaterException.class)
	@Test(expected = WaterException.class)
	public void shouldStubVoidMethod() {
		doThrow(WaterException.class).when(waterSourceMock).doSelfCheck();
		// the same with BDD semantics
		// willThrow(WaterException.class).given(waterSourceMock).doSelfCheck();
		waterSourceMock.doSelfCheck();
	}

	@Test
	public void shouldReturnTheSameValue() {
		given(flowerFilterMock.filterNoOfFlowers(Mockito.anyInt())).will(ReturnFirstArgument.returnFirstArgument());
		int filteredNoOfFlowers = flowerFilterMock.filterNoOfFlowers(TEST_NUMBER_OF_FLOWERS);
		assertEquals(filteredNoOfFlowers, TEST_NUMBER_OF_FLOWERS);
	}

	/**
	 * reusable answer class
	 * 
	 * @author MXPCL Note: The need to use a custom answer may indicate that
	 *         tested code is too complicated and should be re-factored.
	 *
	 */
	public static class ReturnFirstArgument implements Answer<Object> {

		public Object answer(InvocationOnMock invocation) throws Throwable {
			Object[] arguments = invocation.getArguments();
			if (arguments.length == 0) {
				throw new MockitoException("... MockitoException ...");
			}
			return arguments[0];
		}

		public static ReturnFirstArgument returnFirstArgument() {
			return new ReturnFirstArgument();
		}
	}

	/**
	 * verify
	 */
	@Test
	public void verifyMockBehavoir() {
		waterSourceMock.doSelfCheck();
		waterSourceMock.doSelfCheck();
		verify(waterSourceMock, times(2)).doSelfCheck();
		verify(waterSourceMock, never()).getWaterPressure();
		verify(waterSourceMock, atLeast(1)).doSelfCheck();
	}

	/**
	 * Verifying Call Order
	 */
	@Test
	public void shouldVerifyInOrderThroughDifferentMocks() {

		flowerFilterMock.filterNoOfFlowers(TEST_NUMBER_OF_FLOWERS);
		waterSourceMock.doSelfCheck();
		flowerFilterMock.filterNoOfFlowers(TEST_NUMBER_OF_FLOWERS);

		InOrder inOrder = inOrder(flowerFilterMock, waterSourceMock);
		inOrder.verify(flowerFilterMock).filterNoOfFlowers(Mockito.anyInt());
		inOrder.verify(waterSourceMock).doSelfCheck();
		inOrder.verify(flowerFilterMock).filterNoOfFlowers(Mockito.anyInt());
	}

	/**
	 * Verifying With Argument Matching
	 */
	@Test
	public void verifyArgumentMatching() {
		SearchCriteria searchCriteria = new SearchCriteria();
		searchCriteria.setColor("yellow");
		searchCriteria.setNumberOfBuds(3);
		// when
		flowerSearcherMock.findMatching(searchCriteria);
		// then
		ArgumentCaptor<SearchCriteria> captor = ArgumentCaptor.forClass(SearchCriteria.class);
		verify(flowerSearcherMock).findMatching(captor.capture());

		SearchCriteria searchCriteriaUsed = captor.getValue();
		assertEquals(searchCriteriaUsed.getColor(), "yellow");
		assertEquals(searchCriteriaUsed.getNumberOfBuds(), 3);
	}
	
	/**
	 * Verifying With Timeout
	 */

}