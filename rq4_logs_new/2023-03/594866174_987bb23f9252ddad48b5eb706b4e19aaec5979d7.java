package ru.yandex.practicum.filmorate.storage.film;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Repository;
import ru.yandex.practicum.filmorate.model.Film;

import java.util.List;

@Slf4j
@Repository
@ConditionalOnProperty(name = "app.storage.type", havingValue = "db")
public class FilmDbStorage implements FilmStorage {
    public FilmDbStorage (){
        log.debug("Работаем в базе данных");
    }

    @Override
    public Film addObject(Film object) {
        return null;
    }

    @Override
    public Film renewalObject(Film object) {
        return null;
    }

    @Override
    public void deleteObject(long id) {

    }

    @Override
    public Film findObjectById(long id) {
        return null;
    }

    @Override
    public List<Film> getAllObjects() {
        return null;
    }
}