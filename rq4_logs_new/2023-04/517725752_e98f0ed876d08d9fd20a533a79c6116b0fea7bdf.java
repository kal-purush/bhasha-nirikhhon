package com.davidvignon.go4lunch.data;

import static org.junit.Assert.assertEquals;

import androidx.arch.core.executor.testing.InstantTaskExecutorRule;

import com.davidvignon.go4lunch.utils.LiveDataTestUtils;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

public class CurrentQueryRepositoryTest {

    @Rule
    public InstantTaskExecutorRule instantTaskExecutorRule = new InstantTaskExecutorRule();

    private CurrentQueryRepository currentQueryRepository;

    @Before
    public void setUp(){

        currentQueryRepository = new CurrentQueryRepository();
    }

    @Test
    public void test_getCurrentRestaurantQuery(){

        String query  = "Test query";
        currentQueryRepository.setCurrentRestaurantQuery(query);

        String queryMutableLiveDataValue = LiveDataTestUtils.getValueForTesting(currentQueryRepository.getCurrentRestaurantQuery());
        assertEquals(query, queryMutableLiveDataValue);

    }

}