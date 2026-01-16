package ru.otus.changelog;

import com.github.cloudyrock.mongock.ChangeLog;
import com.github.cloudyrock.mongock.ChangeSet;
import com.mongodb.client.MongoDatabase;
import org.springframework.context.annotation.Profile;
import org.springframework.data.mongodb.core.MongoTemplate;
import ru.otus.domain.Author;
import ru.otus.domain.Book;
import ru.otus.domain.Comment;
import ru.otus.domain.Genre;
import ru.otus.security.domain.LibraryUser;

import java.util.Arrays;
import java.util.Collections;

@Profile("dev")
@ChangeLog(order = "001")
public class MongoChangeLog {

    Author pushkin = new Author("Пушкин");
    Author tolstoy = new Author("Толстой");


    private Book capitan = new Book(
            "Капитанская дочка",
            new Genre("Роман"),
            pushkin,
            Arrays.asList(new Comment("Прекрасный роман"), new Comment("Автор затронул исторические и общечеловечные проблемы"))
    );

    private Book gypsies = new Book(
            "Цыганы",
            new Genre("Поэма"),
            pushkin,
            Collections.singletonList(new Comment("Великолепная поэма"))
    );

    private Book warAndPeace = new Book(
            "Война и Мир",
            new Genre("Роман"),
            tolstoy,
            Collections.singletonList(new Comment("Классика"))
    );

    @ChangeSet(author = "vivtanikmv", id = "drop", order = "000", runAlways = true)
    public void dropDatabase(MongoDatabase database) {
        database.drop();
    }

    @ChangeSet(author = "vivtanikmv", id = "init_users", order = "001", runAlways = true)
    public void initUser(MongoTemplate template) {
        template.save(new LibraryUser("admin", "password", "ADMIN"));
        template.save(new LibraryUser("user", "password", "USER"));
    }

    @ChangeSet(author = "vivtanikmv", id = "init_authors", order = "002", runAlways = true)
    public void initAuthors(MongoTemplate template) {
        template.save(pushkin);
        template.save(tolstoy);
    }

    @ChangeSet(author = "vivtanikmv", id = "init_books", order = "003", runAlways = true)
    public void initBooks(MongoTemplate template) {
        template.save(capitan);
        template.save(warAndPeace);
        template.save(gypsies);
    }

}