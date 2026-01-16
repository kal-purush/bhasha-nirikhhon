package ru.otus.spring.dao.jdbc;

import lombok.RequiredArgsConstructor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcOperations;
import org.springframework.stereotype.Repository;
import ru.otus.spring.dao.AuthorDao;
import ru.otus.spring.domain.Author;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

@Repository
@RequiredArgsConstructor
public class AuthorDaoJdbc implements AuthorDao {

    private final NamedParameterJdbcOperations jdbc;

    @Override
    public List<Author> getAuthors() {
        return jdbc.getJdbcOperations().query("SELECT AUTHOR_ID,FULL_NAME FROM AUTHOR", new AuthorMapper());
    }

    @Override
    public Author getAuthorById(long id) {
        return jdbc.queryForObject("SELECT AUTHOR_ID,FULL_NAME FROM AUTHOR WHERE AUTHOR_ID=:id",
                Map.of("id", id), new AuthorMapper());
    }

    public static class AuthorMapper implements RowMapper<Author> {
        @Override
        public Author mapRow(ResultSet rs, int rowNum) throws SQLException {
            long id = rs.getLong("AUTHOR_ID");
            String fullName = rs.getString("FULL_NAME");

            return new Author(id, fullName);
        }
    }
}