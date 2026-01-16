package wolox.training.util;

import wolox.training.models.Book;
import wolox.training.models.User;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class TestEntities {

    public static User mockUser(){
        User oneTestUser = new User();
        oneTestUser.setId(1l);
        oneTestUser.setUsername("luismiguel");
        oneTestUser.setName("Luis Miguel");
        oneTestUser.setBirthdate(LocalDate.of(1993, 11, 23));
        return oneTestUser;
    }

    public static Book mockBook(){
        Book oneTestBook = new Book();
        oneTestBook.setId(1l);
        return oneTestBook;
    }

    public static List<Book> mockManyBook(){
        Book oneTestBook = new Book();
        oneTestBook.setId(1l);

        List<Book> manyTestBooks = new ArrayList<>();
        manyTestBooks.add(oneTestBook);
        return manyTestBooks;
    }

    public static List<User> mockManyUsers(){
        List<User> manyTestUsers = new ArrayList<>();
        manyTestUsers.add(mockUser());
        return manyTestUsers;
    }

}