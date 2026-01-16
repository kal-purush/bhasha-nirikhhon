package com.sg.supersightings.dao;

import com.sg.supersightings.dto.Organization;
import com.sg.supersightings.dto.Super;
import com.sg.supersightings.dto.Superpower;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
@SpringBootTest
class SuperDaoImplTest {
    @Autowired
    SuperDao superDao;

    @Autowired
    SuperpowerDao superpowerDao ;

    @Autowired
    OrganizationDao orgDao;

    public SuperDaoImplTest() {
    }

    @BeforeEach
    void setUp() {
        List<Super> superList = superDao.getAllSuperCharacters();
        for (Super sup: superList) {
            superDao.deleteSuper(sup.getSuperID());
        }

        List<Superpower> superpowersList = superpowerDao.getAllPowers();
        for (Superpower powers: superpowersList) {
            superpowerDao.deleteSuperpowerById(powers.getSuperpowerId());
        }
    }

    @Test
    void addAndGetSuper() {
        //        add superPower
        Superpower superpower1 = new Superpower();
        superpower1.setSuperpowerName("Super speed");
        superpower1 = superpowerDao.addSuperpower(superpower1);

        // Retrieve the superpowerID from the superpower1 object
        int superpowerID = superpower1.getSuperpowerId();

//        is this needed?
//        Superpower fromDao = superpowerDao.getSuperpowerById(superpower1.getSuperpowerId());
//        assertEquals(superpower1, fromDao);

        //    Superhero character #1
        Super superChar = new Super();
        superChar.setSuperName("Shadowcat");
        superChar.setSuperDescription("Accountant during the day and superhero at night");
        superChar.setEvil(false);
        superChar.setSuperpowerID(superpowerID);
//        Add characters
        superChar = superDao.createSuper(superChar);

//      get id from DAO
        Super fromSuperDao1 = superDao.getSuperByID(superChar.getSuperID());
        assertEquals(superChar, fromSuperDao1);
    }

    @Test
    void addSuperAndGetAllSuperCharacters() {
//        add superPower
        Superpower superpower1 = new Superpower();
        superpower1.setSuperpowerName("Super speed");
        superpower1 = superpowerDao.addSuperpower(superpower1);

        // Retrieve the superpowerID from the superpower1 object
        int superpowerID = superpower1.getSuperpowerId();

//        Superpower fromDao = superpowerDao.getSuperpowerById(superpower1.getSuperpowerId());
//        assertEquals(superpower1, fromDao);

//    Superhero character #1
        Super superChar = new Super();
        superChar.setSuperName("Shadowcat");
        superChar.setSuperDescription("Accountant during the day and superhero at night");
        superChar.setEvil(false);
        superChar.setSuperpowerID(superpowerID);
//        Add characters
        superChar = superDao.createSuper(superChar);

////      get id from DAO
//        Super fromSuperDao1 = superDao.getSuperByID(superChar.getSuperID());

//        assertEquals(superChar, fromSuperDao1);

//       villain character #2
        Super superChar2 = new Super();
        superChar2.setSuperID(2);
        superChar2.setSuperName("The Cursed Serpent");
        superChar2.setSuperDescription("Looking for my partner in crime");
        superChar2.setEvil(true);
        superChar2.setSuperpowerID(superpowerID);
//        Add characters
        superChar2 = superDao.createSuper(superChar2);

////      get id from DAO
//        Super fromSuperDao2 = superDao.getSuperByID(superChar2.getSuperID());
//        assertEquals(superChar2, fromSuperDao2);

//        getting all superpowers by ID & checking if list matches the # of supers added
        List<Superpower> superpowersList = superpowerDao.getAllPowers();
        assertEquals(superpowersList.size(), 1);
//      checking that the list contains the objects added
        assertTrue(superpowersList.contains(superpower1));


//        getting all superChar by ID & checking if list matches the # of characters added
        List<Super> superList = superDao.getAllSuperCharacters();
        assertEquals(superList.size(), 2);
//      checking that the list contains the objects added
        assertTrue(superList.contains(superChar));
        assertTrue(superList.contains(superChar2));

    }


    @Test
    void updateSuper() {

    }

    @Test
    void deleteSuper() {
        //        add superPower
        Superpower superpower1 = new Superpower();
        superpower1.setSuperpowerName("Super speed");
        superpower1 = superpowerDao.addSuperpower(superpower1);

        // Retrieve the superpowerID from the superpower1 object
        int superpowerID = superpower1.getSuperpowerId();


////        organization
//        List<Organization> orgsList = orgDao.getAllOrganizations();
//        for (Organization org: orgsList) {
//            orgDao.deleteOrganizationById(org.getOrgId());
//        }
//
//        Organization org = new Organization();
//        org.setOrgName("test name");
//        org.setOrgDes("test des");
//        org.setOrgAddress("test address");
//        org.setOrgNumber("123-123-1234");
//        org.setEvil(false);
//        Organization addedOrg = orgDao.addOrganization(org);
//
//        List<Organization> orgs = orgDao.getAllOrganizations();
//        assertEquals(1, orgs.size());


        //    Superhero character #1
        Super superChar = new Super();
        superChar.setSuperName("Shadowcat");
        superChar.setSuperDescription("Accountant during the day and superhero at night");
        superChar.setEvil(false);
        superChar.setSuperpowerID(superpowerID);
//        Add characters
        superChar = superDao.createSuper(superChar);

        //      get id from DAO
        Super fromSuperDao = superDao.getSuperByID(superChar.getSuperID());
        assertEquals(superChar, fromSuperDao);

        //        getting all superChar by ID & checking if list matches the # of characters added
        List<Super> superList = superDao.getAllSuperCharacters();
        assertEquals(superList.size(), 1);
//      checking that the list contains the objects added
        assertTrue(superList.contains(superChar));

        superDao.deleteSuper(superChar.getSuperID());

        //        getting all superChar by ID & checking if list matches the # of characters added
//        Super removedSuper = superDao.getSuperByID(superChar.getSuperID());
        List<Super> deletedSuperList = superDao.getAllSuperCharacters();
        assertEquals(deletedSuperList.size(), 0);
//      checking that the list deleted the objects and doesn't contain anything
//        assertNull(removedSuper, "Super was removed and list should be empty");


        fromSuperDao = superDao.getSuperByID(superChar.getSuperID());
        assertNull(fromSuperDao);
    }
}