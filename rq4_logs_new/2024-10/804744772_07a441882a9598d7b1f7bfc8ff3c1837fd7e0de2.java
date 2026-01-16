package com.example.DBrequests;


import com.example.model.Admin;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public class AdminSQLRepository {

    private final JdbcTemplate jdbcTemplate;

    public AdminSQLRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    // Метод для создания администратора
    public Admin createAdmin(Admin admin) {
        String sql = "INSERT INTO admin (name, email, address, phone, password, is_active, role) VALUES (?, ?, ?, ?, ?, ?, ?)";
        jdbcTemplate.update(sql, admin.getName(), admin.getEmail(), admin.getAddress(), admin.getPhone(),
                admin.getPassword(), admin.getIsActive(), admin.getRole());
        return findAdminByEmail(admin.getEmail()).orElseThrow(() -> new RuntimeException("Admin not saved"));
    }

    // Метод для поиска администратора по ID
    public Optional<Admin> findAdminById(Long id) {
        String sql = "SELECT * FROM admin WHERE id = ?";
        return jdbcTemplate.query(sql, new Object[]{id}, adminRowMapper()).stream().findFirst();
    }

    // Метод для деактивации администратора
    public int deactivateAdmin(Long id) {
        String sql = "UPDATE admin SET is_active = false WHERE id = ?";
        return jdbcTemplate.update(sql, id);
    }

    // Метод для назначения роли администратору
    public int assignRole(Long id, String role) {
        String sql = "UPDATE admin SET role = ? WHERE id = ?";
        return jdbcTemplate.update(sql, role, id);
    }

    // Метод для обновления профиля администратора
    public int updateAdminProfile(Long id, Admin admin) {
        String sql = "UPDATE admin SET name = ?, address = ?, phone = ? WHERE id = ?";
        return jdbcTemplate.update(sql, admin.getName(), admin.getAddress(), admin.getPhone(), id);
    }

    // Метод для изменения пароля администратора
    public int changeAdminPassword(Long id, String password) {
        String sql = "UPDATE admin SET password = ? WHERE id = ?";
        return jdbcTemplate.update(sql, password, id);
    }

    // Вспомогательный метод для поиска администратора по email
    public Optional<Admin> findAdminByEmail(String email) {
        String sql = "SELECT * FROM admin WHERE email = ?";
        return jdbcTemplate.query(sql, new Object[]{email}, adminRowMapper()).stream().findFirst();
    }

    // RowMapper для преобразования результата SQL-запроса в объект Admin
    private RowMapper<Admin> adminRowMapper() {
        return (rs, rowNum) -> {
            Admin admin = new Admin();
            admin.setId(rs.getLong("id"));
            admin.setName(rs.getString("name"));
            admin.setEmail(rs.getString("email"));
            admin.setAddress(rs.getString("address"));
            admin.setPhone(rs.getString("phone"));
            admin.setPassword(rs.getString("password"));
            admin.setIsActive(rs.getBoolean("is_active"));
            admin.setRole(rs.getString("role"));
            return admin;
        };
    }


}