package com.sg.supersightings.dao;

import com.sg.supersightings.dto.Organization;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class OrganizationDaoImplTest {
    @Autowired
    OrganizationDao orgDao;

    @BeforeEach
    public void setUp() {
        List<Organization> orgs = orgDao.getAllOrganizations();
        for (Organization org: orgs) {
            orgDao.deleteOrganizationById(org.getOrgId());
        }
    }

    @Test
    public void testAddAndGetNewOrganization(){
        Organization org = new Organization();
        org.setOrgName("test name");
        org.setOrgDes("test des");
        org.setOrgAddress("test address");
        org.setOrgNumber("123-123-1234");
        org.setEvil(false);
        Organization addedOrg = orgDao.addOrganization(org);

        Organization fetchedOrg = orgDao.getOrganizationById(addedOrg.getOrgId());

        assertEquals(addedOrg, fetchedOrg);
    }
}