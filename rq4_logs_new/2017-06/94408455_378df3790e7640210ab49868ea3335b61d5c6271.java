package com.ac.hosptial.service;

import com.ac.common.fabric.SmartContractWapper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * Created by zhenchao.bi on 6/27/2017.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
public class HospitalFabricServiceTest {

    @Autowired
    private HospitalFabricService service;

    @Autowired
    private SmartContractWapper smartContractWapper;

    @Test
    public void testInstall() throws Exception {
        System.out.println();

    }
}