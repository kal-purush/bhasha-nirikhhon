package impl;

import db.DB;
import db.DBException;
import lombok.extern.slf4j.Slf4j;
import model.dao.DepartmentDAO;
import model.entities.Department;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.sql.SQLException;

import static db.DB.closeConnection;
import static model.dao.DAOFactory.createDepartmentDAO;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Slf4j
@ExtendWith(MockitoExtension.class)
class DepartmentDAOJDBCTest {

    DepartmentDAO departmentDAO = createDepartmentDAO();

    @Test
    void shouldInsertNewDepartmentCorrectly() throws SQLException {
        log.info("Inserindo no banco o departamento {}", mockDepartment().getName());
        departmentDAO.insert(mockDepartment());
        assertEquals(1, departmentDAO.findById(1).getId());
    }

    @Test
    void shouldThrowsDBExceptionWhenSomethingInDepartmentDataIsInvalid() {
        assertThrows(DBException.class, () ->
            departmentDAO.insert(new Department(0, ""))
        );
    }

    @Test
    void shouldThrowsSQLException() {
        closeConnection();
        assertThrows(SQLException.class, () ->
                departmentDAO.insert(new Department(1, "BOMBOM"))
                );
    }

    private Department mockDepartment() {
        return new Department(
                1,
                "RH"
        );
    }

}