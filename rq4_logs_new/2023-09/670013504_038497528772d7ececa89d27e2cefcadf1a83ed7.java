package com.grievance.dto;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.Date;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

@SpringBootTest
public class CommentDtoTest {
  
  @Test
  void test_commentOutDto_equals() {
    CommentOutDto commentOutDto = new CommentOutDto("Description", "user name", new Date());
    CommentOutDto commentOutDto1 = new CommentOutDto("Description1", "user name", new Date());
    CommentOutDto commentOutDto2 = new CommentOutDto("Description", "user name", new Date());
    
    assertThat(commentOutDto.equals(commentOutDto2));
    assertThat(!commentOutDto.equals(commentOutDto1));
  }
  
  @Test
  void test_commentOutDto_hashcode() {
    CommentOutDto commentOutDto = new CommentOutDto("Description", "user name", new Date());
    CommentOutDto commentOutDto1 = new CommentOutDto("Description1", "user name", new Date());
    CommentOutDto commentOutDto2 = new CommentOutDto("Description", "user name", new Date());
    
    assertEquals(commentOutDto.hashCode(), commentOutDto2.hashCode());
    assertNotEquals(commentOutDto1.hashCode(), commentOutDto.hashCode());
  }
}