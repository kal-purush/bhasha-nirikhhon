package com.example.demo;

import static com.shazam.shazamcrest.matcher.Matchers.sameBeanAs;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.skyscreamer.jsonassert.JSONAssert;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit4.SpringRunner;

import com.example.demo.models.Book;

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class IntegrationTest {
	
	//resource https://www.baeldung.com/rest-template
	@LocalServerPort
    private int port;
    TestRestTemplate restTemplate = new TestRestTemplate();
    HttpHeaders headers = new HttpHeaders();
      
//    @Test
//    public void addBook() throws Exception {
//        Book book=new Book();
//		book.setDescription("stringtest");
//		book.setIsbn("stringtest");
//		book.setTitle("stringtest");
//		HttpEntity<Book> entity = new HttpEntity<Book>(book, headers);
//        ResponseEntity<String> response = restTemplate.exchange(createURLWithPort("/book/add"),HttpMethod.POST, entity, String.class);
//        assertThat(response.getBody(),is("success"));
//    }
    
//    @Test
//    public void testRetrieveBookgetAll() throws Exception {
//        HttpEntity<String> entity = new HttpEntity<String>(null, headers);
//        ResponseEntity<String> response = restTemplate.exchange(
//          createURLWithPort("/book/getAll"), HttpMethod.GET, entity, String.class);
//        String expected = "[{\"identifier\":3,\"version\":0,\"title\":\"string\",\"isbn\":\"string\",\"description\":\"string\"}]";
//        JSONAssert.assertEquals(expected, response.getBody(), false);
//    }
//    
//    @Test
//    public void testRetrieveBookById() throws Exception {
//        HttpEntity<String> entity = new HttpEntity<String>(null, headers);
//        ResponseEntity<String> response = restTemplate.exchange(
//          createURLWithPort("/book/get/3"), HttpMethod.GET, entity, String.class);
//        String expected = "{\"identifier\":3,\"version\":0,\"title\":\"string\",\"isbn\":\"string\",\"description\":\"string\"}";
//        JSONAssert.assertEquals(expected, response.getBody(), false);
//    }
    

    
//    @Test
//    public void updateBook() throws Exception {
//       
//        Book book=new Book();
//		book.setIdentifier(3l);
//		book.setVersion(0l);
//		book.setDescription("string");
//		book.setIsbn("string");
//		book.setTitle("string");
//		headers.add("If-Match", book.getVersion().toString());
//		 HttpEntity<Book> entity = new HttpEntity<Book>(book, headers);
//        ResponseEntity<Book> response = restTemplate.exchange(createURLWithPort("/book/update"), HttpMethod.PUT, entity, Book.class);
//        book.setVersion(1l);
//        assertThat(response.getBody(),sameBeanAs(book));
//    }
//    
    private String createURLWithPort(String uri) {
        return "http://localhost:8080"+ uri;
    }

}