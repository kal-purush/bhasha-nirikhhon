package model.dao.impl;

import db.DBException;
import model.dao.DepartmentDAO;
import model.entities.Department;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static db.DB.closeResultSet;
import static db.DB.closeStatement;
import static java.sql.Statement.RETURN_GENERATED_KEYS;

public class DepartmentDAOJDBC implements DepartmentDAO {

    private final Connection conn;

    public DepartmentDAOJDBC(Connection conn) {
        this.conn = conn;
    }

    @Override
    public void insert(Department department) {
        PreparedStatement st = null;
        try {
            st = conn.prepareStatement("INSERT INTO department (Name) VALUES (?)", RETURN_GENERATED_KEYS);
            st.setString(1, department.getName());

            int rowsAffected = st.executeUpdate();

            if (rowsAffected > 0) {
                ResultSet rs = st.getGeneratedKeys();
                if (rs.next()) {
                    department.setId(rs.getInt(1));
                }
                closeResultSet(rs);
            } else {
                throw new DBException("UNEXPECTED ERROR! NO ROWS AFFECTED!");
            }
        } catch (SQLException e) {
            throw new DBException(e.getMessage());
        } finally {
            closeStatement(st);
        }
    }

    @Override
    public void update(Department department) {
        PreparedStatement st = null;
        try {
            st = conn.prepareStatement("UPDATE department SET Name = ? WHERE Id = ?", RETURN_GENERATED_KEYS);
            st.setString(1, department.getName());
            st.setInt(2, department.getId());

            st.executeUpdate();
        } catch (SQLException e) {
            throw new DBException(e.getMessage());
        } finally {
            closeStatement(st);
        }
    }

    @Override
    public void deleteById(Integer id) {
        PreparedStatement st = null;
        try {
            st = conn.prepareStatement("DELETE FROM department WHERE Id = ?");
            st.setInt(1, id);
            st.executeUpdate();
        } catch (SQLException e) {
            throw new DBException(e.getMessage());
        } finally {
            closeStatement(st);
        }
    }

    @Override
    public Department findById(Integer id) {
        PreparedStatement st = null;
        ResultSet rs = null;
        try {
            st = conn.prepareStatement("SELECT * FROM department WHERE Id = ?");
            st.setInt(1, id);
            rs = st.executeQuery();
            if (rs.next()) {
                return new Department(rs.getInt("Id"), rs.getString("Name"));
            }
            return null;
        } catch (SQLException e) {
            throw new DBException(e.getMessage());
        } finally {
            closeStatement(st);
            closeResultSet(rs);
        }

    }

    @Override
    public List<Department> findAll() {
        PreparedStatement st = null;
        ResultSet rs = null;
        try {
            st = conn.prepareStatement("SELECT * FROM department ORDER BY Name");
            rs = st.executeQuery();

            List<Department> departments = new ArrayList<>();

            while (rs.next()) {
                departments.add(new Department(rs.getInt("Id"), rs.getString("Name")));
            }
            return departments;
        } catch (SQLException e) {
            throw new DBException(e.getMessage());
        } finally {
            closeStatement(st);
            closeResultSet(rs);
        }
    }
}