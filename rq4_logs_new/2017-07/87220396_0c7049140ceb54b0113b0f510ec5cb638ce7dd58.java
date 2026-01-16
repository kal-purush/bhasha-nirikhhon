package com.cfm.tiri.services;

import static org.junit.Assert.*;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.cfm.tiri.domain.Truck;
import com.cfm.tiri.repositories.TruckRepository;

@RunWith(MockitoJUnitRunner.class)
public class TruckServiceTest {
	
	private static final Logger log = Logger.getLogger(TruckServiceTest.class);
	
	private TruckService truckService;
	
	@Mock
	private TruckRepository truckRepository;
	
	@Mock
	private Truck truck;
	
	@Before
	public void setupMock() {
		truckService = new TruckServiceImpl();
		truckService.setTruckRepository(truckRepository);
	}
	
	@Test
	public void testMockCreation() {
		assertNotNull(truckService);
		assertNotNull(truckRepository);
		assertNotNull(truck);
	}
	
	
	@Test
	public void testFindAll() {
		List<Truck> list = new ArrayList<Truck>();
		list.add(new Truck(1, "TEST0001", "Renault", "Magnum", "ËURO", 2004, 440, false));
		list.add(new Truck(2, "TEST0002", "Renault", "Magnum", "ËURO", 2004, 440, false));
		list.add(new Truck(3, "TEST0003", "Renault", "Magnum", "ËURO", 2000, 440, false));
		list.add(new Truck(4, "TEST0004", "Renault", "Magnum", "ËURO", 2012, 440, false));
		list.add(new Truck(5, "TEST0005", "Renault", "Magnum", "ËURO", 2004, 440, false));
		log.info("testFindAll() - findAll() to return " + list.toString());
		when(truckRepository.findAll()).thenReturn(list);
		log.info("testFindAll() - findAll() calling");
		List<Truck> result = (List<Truck>) truckService.findAll();
		log.info("testFindAll() - Verifying findAll() is called at least once");
		verify(truckRepository, atLeastOnce()).findAll();
		log.info("testFindAll() - Asserting that the result is not null or empty and has 5 objects");
		assertEquals(result.size(), 5);
		assertNotNull(result);
		assertFalse(result.isEmpty());
	}
	
	@Test
	public void testGetTruckById() {
		Truck truck = new Truck(2, "TEST0001", "Renault", "Magnum", "EURO", 2004, 440, true);
		log.info("testGetTruckById() - findOne(3) to return " + truck.toString());
		when(truckRepository.findById(2)).thenReturn(truck);
		log.info("testGetTruckById() - findOne(3) calling");
		Truck result = truckService.findById(2);
		log.info("testGetTruckById() - Verifying findOne(3) is called at least once");
		verify(truckRepository, atLeastOnce()).findById(2);
		log.info("testGetTruckById() - Asserting that the result is not null");
		assertNotNull(result);
	}
	
	@Test
	public void testListActiveTrucks() {
		List<Truck> list = new ArrayList<Truck>();
		list.add(new Truck(1, "TEST0001", "Renault", "Magnum", "EURO", 2004, 440, true));
		list.add(new Truck(2, "TEST0002", "Renault", "Magnum", "EURO", 2004, 440, true));
		list.add(new Truck(3, "TEST0003", "Renault", "Magnum", "EURO", 2000, 440, true));
		list.add(new Truck(4, "TEST0004", "Renault", "Magnum", "EURO", 2012, 440, true));
		list.add(new Truck(5, "TEST0005", "Renault", "Magnum", "EURO", 2004, 440, true));
		
		
		when(truckRepository.findByActive(true)).thenReturn(list);
		log.info("findByActive() - findByActive(true) calling");
		List<Truck> result = (List<Truck>) truckService.findByActive(true);
		
		for (int i =0; i<result.size(); i++){
			assertEquals(result.get(i).getActive(), true);
		}
		
		
		log.info("findByActive() - Verifying findByActive(true) is called at least once");
		verify(truckRepository, atLeastOnce()).findByActive(true);
		log.info("findByActive() - Asserting that the result is not null or empty");
		assertNotNull(result);
		assertFalse(result.isEmpty());
		
	}


}