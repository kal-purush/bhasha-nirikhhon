package daoTests;

import daos.registry.RegistryDao;
import daos.registry.RegistryDaoPostgres;
import entities.Registry;
import org.junit.jupiter.api.*;
import util.ConnectUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RegistryDaoTests {

    static RegistryDao registryDao = new RegistryDaoPostgres();

    @BeforeAll
    static void connect(){
        try (Connection conn = ConnectUtil.getConnection()) {
            String sql = "delete from registry;\n" +
                    "insert into registry (registry_id, first_name, last_name, access_role, login, log_pass) values (1, 'John', 'Smith', 'COUNCILOR', 'JSmith22', 'RunThisTown!')";
            PreparedStatement ps;
            if (conn != null) {
                ps = conn.prepareStatement(sql);
                ps.execute();
            }


        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    @Order(1)
    void get_person_by_login_test_standard (){
        String testLogin = "JSmith22";
        String testPass = "RunThisTown!";
        Registry result = registryDao.getPersonByLogin(testLogin, testPass);

        Assertions.assertEquals("John", result.getFirstName());
    }
}