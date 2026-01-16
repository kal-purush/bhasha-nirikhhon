package com.danielme.spring.test;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import static org.junit.Assert.assertEquals;

@RunWith(SpringJUnit4ClassRunner.class)
@ActiveProfiles(MockConfiguration.MOCK_PROFILE)
@ContextConfiguration(classes = {AppConfiguration.class})
public class DependencyMockedTest {

    @Autowired
    private Dependency dependency;

    @Test
    public void testSubdependency() {
        assertEquals("mocked", dependency.getSubdepedencyClassName());
    }

}