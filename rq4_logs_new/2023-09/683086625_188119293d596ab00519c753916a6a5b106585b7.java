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

import com.singo.Hotel.HotelRepository;
import com.singo.Hotel.HotelModel;
import com.singo.Hotel.HotelService;

@RunWith(SpringRunner.class)
@SpringBootTest
public class HotelTests {

    @Autowired
    private HotelService service;

    @MockBean
    private HotelRepository repository;

    byte[] bytes = new byte[]{ 1, 2 };

    HotelModel model1 = new HotelModel(1,"testName1", 1, "testLocation1", 100,
     bytes, "testAddress1","testDescription1","testPhone1","testHours1");

    HotelModel model2 = new HotelModel(2,"testName2", 2, "testLocation2", 200,
    bytes, "testAddress2","testDescription2","testPhone2","testHours2");

    MockMultipartFile file = new MockMultipartFile("image", new byte[1]);

    //test get all
    @Test
    public void getAllHotelTest(){
        when(repository.selectAllHotel()).thenReturn(Stream
        .of(model1,model2).collect(Collectors.toList()));
		assertEquals(2, service.getAllHotel().count());
	}
    
    //test add
    @Test
	public void addHotelTest() throws IOException {
		service.addHotelFile(model1,file);
        verify(repository, times(1)).insertHotel(model1);
	}

    //test update
     @Test
	public void updateHotelTest() throws IOException {
		service.updateHotelFile(model1,file);
        verify(repository, times(1)).updateHotel(model1);
	}

    //test delete
    @Test
	public void deleteHotelTest() {
		service.deleteHotel(1);
		verify(repository, times(1)).deleteHotel(1);
	}
    
    @Test
	public void getBytesHotelTest() {
		String name = "testName1";
		when(repository.findHotelImage(name))
				.thenReturn(Stream.of(model1).collect(Collectors.toList()));
		assertEquals(bytes, service.getHotelImage(name));
	}
    
}