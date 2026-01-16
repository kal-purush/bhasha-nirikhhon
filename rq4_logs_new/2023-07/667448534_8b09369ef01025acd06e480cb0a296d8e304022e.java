package com.sg.supersightings.dao;

import com.sg.supersightings.dto.Organization;
import com.sg.supersightings.dto.Super;
import com.sg.supersightings.dto.Superpower;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class SuperpowerDaoImplTest {
    @Autowired
    SuperpowerDao superpowerDao;
    @Autowired
    SuperDao superDao;


    public SuperpowerDaoImplTest(){

    }
    @BeforeAll
    public static void setUpClass() {
    }

    @AfterAll
    public static void tearDownClass() {
    }
    @BeforeEach
    void setUp() {
        List<Superpower> superpowers = superpowerDao.getAllPowers() ;
        for (Superpower power: superpowers) {
            superpowerDao.deleteSuperpowerById(power.getSuperpowerId());
        }

        List<Super> supers = superDao.getAllSuperCharacters();
        for (Super superCharater : supers){
            superDao.deleteSuper(superCharater.getSuperID());
        }
    }
    @AfterEach
    void tearDown() {
    }




    /**
     * Test of getSuperpowerById method, of class GameDaoImpl.
     */
    @Test
    void testGetSuperpowerByIdAndAdd() {
        Superpower superpower = new Superpower();
        superpower.setSuperpowerName("Eatting");
        superpower = superpowerDao.addSuperpower(superpower);

        Superpower fromDao = superpowerDao.getSuperpowerById(superpower.getSuperpowerId());
        assertEquals(superpower, fromDao);
    }


    /**
     * Test of getAllPowers method, of class PowerDaoDB.
     */
    @Test
    void getAllPowers() {
        Superpower superpower1 = new Superpower();
        superpower1.setSuperpowerName("Food");
        superpower1 = superpowerDao.addSuperpower(superpower1);


        Superpower superpower2 = new Superpower();
        superpower2.setSuperpowerName("sleeping");
        superpower2 = superpowerDao.addSuperpower(superpower2);

        List<Superpower> superpowers = superpowerDao.getAllPowers();
        assertEquals(2, superpowers.size());
        assertTrue(superpowers.contains(superpower1));
        assertTrue(superpowers.contains(superpower2));
    }


    /**
     * Test of updateSuperpowers method, of class PowerDaoDB.
     */
    @Test
    void updateSuperpower() {
    }
    /**
     * Test of deleteSuperpowerById method, of class PowerDaoDB.
     */
    @Test
    void deleteSuperpowerById() {
    }
}