package daoTests;


import daos.complaint.ComplaintDao;
import daos.complaint.ComplaintDaoPostgres;
import entities.Complaint;
import org.junit.jupiter.api.*;
import util.ConnectUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.Callable;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class ComplaintDaoTests {

    static ComplaintDao complaintDao= new ComplaintDaoPostgres();

    @BeforeAll
    static void connect() {
        try(Connection conn = ConnectUtil.getConnection()) {
            String sql = "delete from complaints";
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.execute();

        }catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Test
    @Order(1)
    void create_complaint_test_standard() {
        Complaint test = new Complaint("test", "test", "testing");
        int result = complaintDao.createComplaint(test);

        Assertions.assertEquals(201, result);
    }



}