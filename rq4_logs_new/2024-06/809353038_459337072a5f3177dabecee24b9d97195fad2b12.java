package com.todo.todoapp;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Date;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase.Replace;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.test.context.ActiveProfiles;

import com.todo.category.Category;

@DataJpaTest
@AutoConfigureTestDatabase(replace = Replace.NONE)
@ActiveProfiles("test")
public class TodoRepositoryTest {

    @Autowired
    private TestEntityManager entityManager;

    @Autowired
    private TodoRepository todoRepository;

    private Category category;
    private Todo todo;

    @BeforeEach
    void setUp() {
        // Setting up a Category entity
        category = new Category();
        category.setName("Work");
        entityManager.persist(category);

        // Setting up a Todo entity
        todo = new Todo();
        todo.setTitle("New Todo");
        todo.setDescription("This is a new todo item");
        todo.setCategory(category);
        todo.setCompleted(false);
        todo.setCreatedAt(new Date());
        todo.setUpdatedAt(new Date());
        entityManager.persist(todo);
        entityManager.flush();
    }

    @Test
    public void whenFindById_thenReturnTodo() {
        // when
        Todo found = todoRepository.findById(todo.getId()).orElse(null);

        // then
        assertThat(found).isNotNull();
        assertThat(found.getTitle()).isEqualTo(todo.getTitle());
    }

}