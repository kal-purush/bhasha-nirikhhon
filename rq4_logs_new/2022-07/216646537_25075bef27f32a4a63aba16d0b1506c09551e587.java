package com.mjamsek.rest.test.tests;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mjamsek.rest.dto.EntityWithMap;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.jboss.arquillian.container.test.api.Deployment;
import org.jboss.arquillian.junit.Arquillian;
import org.jboss.shrinkwrap.api.ShrinkWrap;
import org.jboss.shrinkwrap.api.asset.EmptyAsset;
import org.jboss.shrinkwrap.api.spec.JavaArchive;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(Arquillian.class)
public class EntityWithMapTest {
    
    private static final TestEntity TEST_ENTITY;
    private static final String TEST_NAME = "John";
    
    private static final String ANOTHER_FIELD_NAME = "another";
    private static final String ANOTHER_FIELD_VALUE = "test";
    private static final int TEST_AGE = 31;
    
    private static final String TEST_SIMPLE_OBJECT_STRING = "{\"age\":31,\"name\":\"John\"}";
    private static final String TEST_OBJECT_STRING = "{\"age\":31,\"name\":\"John\",\"another\":\"test\"}";
    
    // private static ObjectMapper objectMapper;
    
    static {
        TEST_ENTITY = new TestEntity();
        TEST_ENTITY.setAge(TEST_AGE);
        TEST_ENTITY.setName(TEST_NAME);
    }
    
    @Deployment
    public static JavaArchive createDeployment() {
        return ShrinkWrap.create(JavaArchive.class)
            .addClass(EntityWithMap.class)
            .addClass(TestEntity.class)
            .addClass(ObjectMapper.class)
            .addClass(JacksonFeature.class)
            .addAsManifestResource(EmptyAsset.INSTANCE, "beans.xml");
    }
    
    @Before
    public void init() {
        // objectMapper = new ObjectMapper();
    }
    
    @Test
    public void construction() {
        EntityWithMap<TestEntity> entity = new EntityWithMap<>();
        entity.setEntity(TEST_ENTITY);
        entity.setOptions(ANOTHER_FIELD_NAME, ANOTHER_FIELD_VALUE);
        entity.setOptions(ANOTHER_FIELD_NAME + "2", ANOTHER_FIELD_VALUE + "2");
        
        assertNotNull(entity.getEntity());
        assertNotNull(entity.getOptions());
        
        assertEquals(TEST_AGE, entity.getEntity().getAge());
        assertEquals(TEST_NAME, entity.getEntity().getName());
        assertNotNull(entity.getOptions().get(ANOTHER_FIELD_NAME + "2"));
        assertEquals(ANOTHER_FIELD_VALUE + "2", entity.getOptions().get(ANOTHER_FIELD_NAME + "2"));
    }
    
    /*@Test
    public void simpleDeserialization() throws Exception {
        EntityWithMap<TestEntity> entity = objectMapper.readValue(TEST_SIMPLE_OBJECT_STRING, EntityWithMap.class);
        assertNotNull(entity.getEntity());
        assertNotNull(entity.getOptions());
        assertEquals(0, entity.getOptions().size());
        
        assertEquals(TEST_AGE, entity.getEntity().getAge());
        assertEquals(TEST_NAME, entity.getEntity().getName());
    }
    
    @Test
    public void deserialization() throws Exception {
        EntityWithMap<TestEntity> entity = objectMapper.readValue(TEST_OBJECT_STRING, EntityWithMap.class);
        assertNotNull(entity.getEntity());
        assertNotNull(entity.getOptions());
        
        assertEquals(TEST_AGE, entity.getEntity().getAge());
        assertEquals(TEST_NAME, entity.getEntity().getName());
        assertNotNull(entity.getOptions().get(ANOTHER_FIELD_NAME));
        assertEquals(ANOTHER_FIELD_VALUE, entity.getOptions().get(ANOTHER_FIELD_NAME));
    }*/
    
    public static class TestEntity {
        private String name;
        private int age;
        
        public String getName() {
            return name;
        }
        
        public void setName(String name) {
            this.name = name;
        }
        
        public int getAge() {
            return age;
        }
        
        public void setAge(int age) {
            this.age = age;
        }
    }
    
}