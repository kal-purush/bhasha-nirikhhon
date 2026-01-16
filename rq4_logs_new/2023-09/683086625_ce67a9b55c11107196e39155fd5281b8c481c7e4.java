package com.singo;


import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;
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

import com.singo.Restaurant.RestaurantRepository;
import com.singo.Restaurant.RestaurantModel;
import com.singo.Restaurant.RestaurantService;

@RunWith(SpringRunner.class)
@SpringBootTest
public class RestaurantTests {

    @Autowired
    private RestaurantService service;

    @MockBean
    private RestaurantRepository repository;

    byte[] bytes = new byte[]{ 1, 2 };

    RestaurantModel model1 = new RestaurantModel(1,"testName1", "testType1", "testLocation1", "$",
     bytes, "testAddress1","testDescription1","testPhone1","testHours1");

    RestaurantModel model2 = new RestaurantModel(2,"testName2", "testType2", "testLocation2", "$$",
    bytes, "testAddress2","testDescription2","testPhone2","testHours2");

    MockMultipartFile file = new MockMultipartFile("image", new byte[1]);

    //test get all
    @Test
    public void getAllRestaurantTest(){
        when(repository.selectAllRestaurant()).thenReturn(Stream
        .of(model1,model2).collect(Collectors.toList()));
		assertEquals(2, service.getAllRestaurant().count());
	}
    
    //test add
    @Test
	public void addRestaurantTest() throws IOException {
		service.addRestaurantFile(model1,file);
        verify(repository, times(1)).insertRestaurant(model1);
	}

    //test update
     @Test
	public void updateRestaurantTest() throws IOException {
		service.updateRestaurantFile(model1,file);
        verify(repository, times(1)).updateRestaurant(model1);
	}

    //test delete
    @Test
	public void deleteRestaurantTest() {
		service.deleteRestaurant(1);
		verify(repository, times(1)).deleteRestaurant(1);
	}
    
    @Test
	public void getBytesRestaurantTest() {
		String name = "testName1";
		when(repository.findRestaurantImage(name))
				.thenReturn(Stream.of(model1).collect(Collectors.toList()));
		assertEquals(bytes, service.getRestaurantImage(name));
	}
    
}