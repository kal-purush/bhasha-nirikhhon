package com.kodilla.ecommerce.domain;

import com.kodilla.ecommerce.repository.GroupRepository;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(SpringExtension.class)
@SpringBootTest

public class GroupTest {

    @Autowired
    private GroupRepository groupRepository;

    @Test
    public void testSaveGroup() {

        //Given
        Group groupTest1 = new Group();
        groupTest1.setName("Odzież");
        //When
        groupRepository.save(groupTest1);
        Long groupTest1Id = groupTest1.getGroupId();
        Optional<Group> testGroupId = groupRepository.findById(groupTest1Id);
        //Then
        assertTrue(groupRepository.existsById(groupTest1Id));
        assertTrue(testGroupId.isPresent());
        //Clean-up
        groupRepository.deleteById(groupTest1Id);
    }

    @Test
    public void testSetNameGroup() {
        //Given
        Group groupTest2 = new Group();
        groupTest2.setName("Obuwie");
        //When
        groupRepository.save(groupTest2);
        String name = groupTest2.getName();
        //Then
        assertEquals("Obuwie", name);
        //CleanUp
        groupRepository.deleteAll();
    }

    @Test
    public void testSetProducts() {
        //Given
        Group groupTest1 = new Group();
        Group groupTest2 = new Group();
        Product product1 = new Product();
        product1.setName("product 1");
        product1.setGroup(groupTest1);
        Product product2 = new Product();
        product2.setName("product 2");
        product2.setGroup(groupTest2);

        //When
        groupTest1.getProducts().add(product1);
        groupTest2.getProducts().add(product2);

        Group savedGroupTest1 = groupRepository.save(groupTest1);
        Group savedGroupTest2 = groupRepository.save(groupTest2);
        //Then
        assertTrue(savedGroupTest1.getProducts().contains(product1));
        assertTrue(savedGroupTest2.getProducts().contains(product2));

        //CleanUp
        groupRepository.deleteAll();
    }

}