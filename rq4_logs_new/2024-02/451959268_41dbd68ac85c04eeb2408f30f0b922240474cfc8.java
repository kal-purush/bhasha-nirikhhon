package pl.todolist.repository;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import pl.todolist.model.ToDoItem;

import java.sql.Date;
import java.util.List;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

@DataJpaTest
class ToDoItemRepositoryTest {

    @Autowired
    private ToDoItemRepository repository;

    @Test
    @DisplayName("Should return true if item exists by id")
    void existsByIdIfExists() {
        Date testDate = new Date(1000000);
        ToDoItem item = new ToDoItem("Test item", "Used only for testing", testDate);

        repository.save(item);
        boolean existsById = repository.existsById(item.getId());

        assertThat(existsById).isTrue();
    }

    @Test
    @DisplayName("Should return false if item does not exist by id")
    void existsByIdIfDoesNotExist() {
        Date testDate = new Date(1000000);
        ToDoItem item = new ToDoItem("Test item", "Used only for testing", testDate);

        boolean existsById = repository.existsById(item.getId());

        assertThat(existsById).isFalse();
    }

    @Test
    @DisplayName("Should return 1 if an item exists by id")
    void deleteByIdWhenExists() {
        Date testDate = new Date(1000000);
        ToDoItem item = new ToDoItem("Test item", "Used only for testing", testDate);

        repository.save(item);
        int numberOfDeletedItems = repository.deleteById(item.getId());

        assertThat(numberOfDeletedItems).isEqualTo(1);
    }

    @Test
    @DisplayName("Should return 0 if an item does not exist by id")
    void deleteByIdWhenDoesNotExist() {
        Date testDate = new Date(1000000);
        ToDoItem item = new ToDoItem("Test item", "Used only for testing", testDate);

        int numberOfDeletedItems = repository.deleteById(item.getId());

        assertThat(numberOfDeletedItems).isEqualTo(0);
    }

    @Test
    @DisplayName("Should return an empty list if table empty")
    void findAllByOrderByDeadlineWhenEmptyTable() {
        List<ToDoItem> allItems = repository.findAllByOrderByDeadline();

        assertThat(allItems).isEmpty();
    }

    @Test
    @DisplayName("Should return a list of all items ordered ascending by deadline")
    void findAllByOrderByDeadline() {
        Date testDate1 = new Date(100000000);
        Date testDate2 = new Date(200000000);
        Date testDate3 = new Date(300000000);
        ToDoItem item2 = repository.save(new ToDoItem("Test item2", "Used only for testing", testDate2));
        ToDoItem item1 = repository.save(new ToDoItem("Test item1", "Used only for testing", testDate1));
        ToDoItem item3 = repository.save(new ToDoItem("Test item3", "Used only for testing", testDate3));
        List<ToDoItem> expected = List.of(item1, item2, item3);

        List<ToDoItem> allByOrderByDeadline = repository.findAllByOrderByDeadline();

        assertThat(allByOrderByDeadline).isEqualTo(expected);
    }
}