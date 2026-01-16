package com.mjamsek.rest.test.tests;

import com.mjamsek.rest.dto.EntityList;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class EntityListTest {
    
    private static final List<String> EMPTY_LIST = new ArrayList<>();
    private static final List<String> TEST_LIST = List.of("one", "two", "three");
    
    @Test
    public void emptyLists() {
        EntityList<String> list1 = new EntityList<>();
        
        assertEquals(0, list1.getCount());
        assertNull(list1.getLimit());
        assertNull(list1.getOffset());
        assertNotNull(list1.getEntities());
        assertEquals(0, list1.getEntities().size());
    
        EntityList<String> list2 = new EntityList<>(EMPTY_LIST);
        assertEquals(0, list2.getCount());
        assertNull(list2.getLimit());
        assertNull(list2.getOffset());
        assertNotNull(list2.getEntities());
        assertEquals(0, list2.getEntities().size());
    }
    
    @Test
    public void nonEmptyList() {
        EntityList<String> list = new EntityList<>(TEST_LIST);
        assertEquals(TEST_LIST.size(), list.getCount());
        assertNull(list.getLimit());
        assertNull(list.getOffset());
        assertNotNull(list.getEntities());
        assertEquals(TEST_LIST.size(), list.getEntities().size());
    }
    
    @Test
    public void nonEmptyListWithCount() {
        EntityList<String> list = new EntityList<>(TEST_LIST, 55);
        assertEquals(55, list.getCount());
        assertNull(list.getLimit());
        assertNull(list.getOffset());
        assertNotNull(list.getEntities());
        assertEquals(TEST_LIST.size(), list.getEntities().size());
    }
    
    @Test
    public void nonEmptyListWithAllParams() {
        EntityList<String> list = new EntityList<>(TEST_LIST, 55, 25L, 35L);
        assertEquals(55L, list.getCount());
        assertEquals(25L, (long)list.getOffset());
        assertEquals(35L, (long)list.getLimit());
        assertNotNull(list.getEntities());
        assertEquals(TEST_LIST.size(), list.getEntities().size());
    }
    
    @Test
    public void immutableList() {
        List<String> mutableList = new ArrayList<>(TEST_LIST);
        EntityList<String> list = new EntityList<>(mutableList);
    
        list.getEntities().add("new new");
        assertEquals(TEST_LIST, list.getEntities());
        assertEquals(TEST_LIST.size(), list.getCount());
    }
    
}