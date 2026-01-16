package com.singo;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.context.junit4.SpringRunner;

import com.singo.Activities.ActivitiesRepository;
import com.singo.Activities.ActivitiesModel;
import com.singo.Activities.ActivitiesService;

@RunWith(SpringRunner.class)
@SpringBootTest
public class AdminActivitiesTests {

    @Autowired
    private ActivitiesService service;

    @MockBean
    private ActivitiesRepository repository;

    byte[] bytes = new byte[]{ 1, 2 };

    ActivitiesModel model1 = new ActivitiesModel(1,"testName1", "testType1", "testLocation1", 100,
     bytes, "testAddress1","testDescription1","testPhone1","testHours1");

    ActivitiesModel model2 = new ActivitiesModel(2,"testName2", "testType2", "testLocation2", 200,
    bytes, "testAddress2","testDescription2","testPhone2","testHours2");

    MockMultipartFile file = new MockMultipartFile("image", new byte[1]);

    //test get all
    @Test
    public void getAllActivitiesTest(){
        when(repository.selectAllActivities()).thenReturn(Stream
        .of(model1,model2).collect(Collectors.toList()));
		assertEquals(2, service.getAllActivities().count());
	}
    
    //test add
    @Test
	public void addActivitiesTest() throws IOException {
		service.addActivitiesFile(model1,file);
        verify(repository, times(1)).insertActivities(model1);
	}

    //test update
     @Test
	public void updateActivitiesTest() throws IOException {
		service.updateActivitiesFile(model1,file);
        verify(repository, times(1)).updateActivities(model1);
	}

    //test delete
    @Test
	public void deleteActivitiesTest() {
		service.deleteActivities(1);
		verify(repository, times(1)).deleteActivities(1);
	}
    
    @Test
	public void getBytesActivitiesTest() {
		String name = "testName1";
		when(repository.findActivitiesImage(name))
				.thenReturn(Stream.of(model1).collect(Collectors.toList()));
		assertEquals(bytes, service.getActivitiesImage(name));
	}
    
}