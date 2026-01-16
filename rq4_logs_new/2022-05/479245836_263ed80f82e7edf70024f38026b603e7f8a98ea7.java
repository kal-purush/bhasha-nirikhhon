package ru.tasklist.springboot.auth.utils;

import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.tasklist.springboot.auth.entity.User;

import java.util.Date;

@Log4j2
@Component
public class JwtUtils {

    @Value("${jwt.secret}")
    private String jwtSecret; // секретный ключ для создания jwt (хранится только на сервере, нельзя никуда передавать)


    @Value("${jwt.access_token-expiration}") // 86400000 мс = 1 сутки
    private int accessTokenExpiration; // длительность токена для автоматического логина (все запросы будут автоматически проходить аутентификацию, если в них присутствует JWT)
    // название взяли по аналогии с протоколом OAuth2, но не путайте - это просто название нашего JWT, здесь мы не применяем OAuth2

    // генерация JWT по данным пользователя
    public String createAccessToken(User user) {

        Date currentDate = new Date(); // для отсчета времени от текущего момента - для задания expiration

        return Jwts.builder()

                // задаем claims
                // Какие именно данные (claims) добавлять в JWT (решаете сами)
                .setSubject((user.getId().toString())) // subject - это одно из стандартных полей jwt (сохраняем неизменяемое id пользователя)
                .setIssuedAt(currentDate) // время отсчета - текущий момент
                .setExpiration(new Date(currentDate.getTime() + accessTokenExpiration)) // срок действия access_token

                .signWith(SignatureAlgorithm.HS512, jwtSecret) // используем алгоритм кодирования HS512 (часто используемый в соотношении скорость-качество) - хешируем все данные секретным ключом-строкой
                .compact(); // кодируем в формат Base64 (это не шифрование, а просто представление данных в виде удобной строки)
    }

}