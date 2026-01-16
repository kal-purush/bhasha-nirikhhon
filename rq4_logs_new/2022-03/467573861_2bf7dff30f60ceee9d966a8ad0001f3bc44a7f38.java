package ru.platon.bot2.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.PropertySource;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Service;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.sql.Connection;
import java.sql.DriverManager;

@Component
@Slf4j
@PropertySource("classpath:application.properties")
public class ConnectBD {

    @PersistenceContext
    private static EntityManager entityManager;

    /**
     * Метод принимает строку запроса и выполняет ее, после чего присылает результат
     *
     * @param strSQL строку SQL запроса
     */
    public static void connectAndExecute(String strSQL) {
        entityManager.createNativeQuery(strSQL)
                .executeUpdate();
    }
}