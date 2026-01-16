package entities;

import model.entities.Department;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(MockitoExtension.class)
class DepartmentTest {

    @Test
    void shouldTestGettersAndSettersAndToString() {
        //GIVEN
        Department expected = mockDepartment();

        //WHEN
        Department actual = new Department();
        actual.setId(1);
        actual.setName("Tecnologia da Informação");

        //THEN
        assertEquals(expected, actual);
        assertEquals(expected.getId(), actual.getId());
        assertEquals(expected.getName(), actual.getName());
        assertEquals(expected.toString(), actual.toString());
    }

    @Test
    void shouldTestHashCode() {
        //GIVEN
        Department expected = mockDepartment();

        //WHEN
        Department actual = new Department(1, "Tecnologia da Informação");

        //THEN
        assertEquals(expected.hashCode(), actual.hashCode());
    }

    private Department mockDepartment() {
        return new Department(
                1,
                "Tecnologia da Informação");
    }

}