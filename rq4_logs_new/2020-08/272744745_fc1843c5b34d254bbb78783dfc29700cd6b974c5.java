package com.dungeongroupfinder;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@SpringBootTest
public class TableDataValidityTest extends GuildFinderIntegrationTest {

    @Test
    public void testTablesExistance() {
        List<String> expectedTableList = new ArrayList<String>();
        expectedTableList.add("guilds");
        expectedTableList.add("players");
        assertEquals(expectedTableList, playerRepository.getTables());
    }
}