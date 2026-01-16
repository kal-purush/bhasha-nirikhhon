package ru.grinick.chess.dao;


import org.springframework.data.jpa.repository.JpaRepository;
import ru.grinick.chess.model.Person;

public interface PersonRepository extends JpaRepository <Person, Long> {}