package com.example.itemservice.service;

import com.example.itemservice.dto.ItemDto;
import com.example.itemservice.enums.ItemCategory;
import com.example.itemservice.model.Item;
import com.example.itemservice.repository.ItemRepository;
import com.example.itemservice.util.ItemMapper;
import org.checkerframework.checker.nullness.Opt;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.test.context.event.annotation.BeforeTestMethod;

import javax.persistence.Access;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;

class ItemServiceTest {

    @InjectMocks
    private ItemService itemService;

    @Mock
    private ItemRepository itemRepository;
    @Mock
    private ItemMapper itemMapper;
    private static final Item item;
    private static final ItemDto itemDto;

    static {
        item = Item.builder()
                .itemId(1L)
                .userId("123-09")
                .name("name")
                .description("description")
                .categories(List.of(ItemCategory.PHONE,ItemCategory.LAPTOP))
                .build();
        itemDto = ItemDto.builder()
                .itemId(1L)
                .userId("123-09")
                .name("name")
                .description("description")
                .categories(List.of(ItemCategory.PHONE,ItemCategory.LAPTOP))
                .build();
    }




    @BeforeEach
    void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);
    }


    @BeforeEach
    void itemMapper(){
        Mockito.when(itemMapper.fromItem(item))
                .thenReturn(itemDto);
        Mockito.when(itemMapper.toItem(itemDto))
                .thenReturn(item);
    }




    @Test
    public void getAnItemByIdFromRepoAndReturnAnItemDto(){
        Mockito.when(itemRepository.findById(1L))
                .thenReturn(Optional.of(item));
        ItemDto result = itemService.getItemById(1L);

        assertEquals(itemDto,result);
    }

    @Test
    public void createAnNewItemAndReturnAnItemDto(){
        Mockito.when(itemRepository.save(item))
                .thenReturn(item);
        ItemDto result = itemService.createItem(itemDto);
        assertEquals(itemDto,result);


    }


}