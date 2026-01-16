package com.kodilla.ecommercee;

import com.kodilla.ecommercee.domain.Group;
import com.kodilla.ecommercee.domain.Product;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@RunWith(SpringRunner.class)
@SpringBootTest
@Transactional
public class GroupRepositoryTestSuite {
    @Autowired
    private GroupRepository groupRepository;

    @Test
    public void testSaveGroup() {
        //Given
        Group groupOne = new Group("groupOne");
        //When
        groupRepository.save(groupOne);
        Long groupOneId = groupOne.getId();
        //Then
        Assert.assertNotEquals(0, (long) groupOneId);
    }

    @Test
    public void testSaveGroupWithProducts() {
        //Given
        Product productOne = new Product();
        Product productTwo = new Product();
        List<Product> products = new ArrayList<>();
        products.add(productOne);
        products.add(productTwo);
        Group groupOne = new Group(1L, "groupOne", products);
        //When
        groupRepository.save(groupOne);
        Optional<Group> groupOptional = groupRepository.findById(1L);
        int productsSize = 0;
        if (groupOptional.isPresent()) {
            productsSize = groupOptional.get().getProducts().size();
        }
        //Then
        Assert.assertEquals(2, productsSize);
    }

    @Test
    public void testFindAllGroups() {
        //Given
        Group groupOne = new Group("groupOne");
        Group groupTwo = new Group("groupTwo");
        groupRepository.save(groupOne);
        groupRepository.save(groupTwo);
        //When
        List<Group> groups = groupRepository.findAll();
        //Then
        Assert.assertEquals(2, groups.size());
    }

    @Test
    public void testFindGroupById() {
        //Given
        Group groupOne = new Group("groupOne");
        Group groupTwo = new Group("groupTwo");
        groupRepository.save(groupOne);
        Long groupOneId = groupOne.getId();
        groupRepository.save(groupTwo);
        //When
        Optional<Group> groupFound = groupRepository.findById(groupOneId);
        //Then
        Assert.assertTrue(groupFound.isPresent());
    }

    @Test
    public void testDeleteGroupById() {
        //Given
        Group groupOne = new Group("groupOne");
        Group groupTwo = new Group("groupTwo");
        groupRepository.save(groupOne);
        Long groupOneId = groupOne.getId();
        groupRepository.save(groupTwo);
        //When
        groupRepository.deleteById(groupOneId);
        Optional<Group> groupFound = groupRepository.findById(groupOneId);
        //Then
        Assert.assertFalse(groupFound.isPresent());
    }
}