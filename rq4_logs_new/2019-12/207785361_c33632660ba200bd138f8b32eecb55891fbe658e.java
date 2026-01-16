package ru.otus.spring.homework14.domain.mongo;

import lombok.*;
import ru.otus.spring.homework14.domain.sql.GenreSql;

import javax.persistence.*;

@Data
@RequiredArgsConstructor
@AllArgsConstructor
@NoArgsConstructor
public class GenreMongo {
    @Id
    private long id;

    @NonNull
    private String name;

    @Override
    public String toString() {
        return "id = " + id +
                ", жанр = '" + name + "'";
    }

    public static GenreMongo convertToDomain(GenreSql genreSql) {
        long id = genreSql.getId();
        String name = genreSql.getName();
        return new GenreMongo(id,name);
    }
}