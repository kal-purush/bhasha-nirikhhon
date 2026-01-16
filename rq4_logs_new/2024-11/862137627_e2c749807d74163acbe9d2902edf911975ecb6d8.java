package org.example.backend.configs;

import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Statement;

@Configuration
public class DatabaseInitializer {

    @Bean
    CommandLineRunner initDatabase(DataSource dataSource) {
        return args -> {
            try (Connection connection = dataSource.getConnection()) {
                try (Statement statement = connection.createStatement()) {
                    // Kiểm tra và cập nhật hoặc thêm người dùng nếu cần
                    statement.execute("""
                        IF EXISTS (SELECT 1 FROM nguoi_dung WHERE id = '58758953-af31-4b8d-a459-8a6f23248bdc')
                        BEGIN
                            UPDATE nguoi_dung
                            SET ten = N'Khách lẻ'
                            WHERE id = '58758953-af31-4b8d-a459-8a6f23248bdc';
                        END
                        ELSE
                        BEGIN
                            INSERT INTO nguoi_dung (id, ten)
                            VALUES ('58758953-af31-4b8d-a459-8a6f23248bdc', N'Khách lẻ');
                        END
                    """);
                }
            }
        };
    }
}