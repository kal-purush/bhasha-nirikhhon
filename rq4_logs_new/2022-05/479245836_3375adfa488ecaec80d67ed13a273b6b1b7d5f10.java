package ru.tasklist.springboot.auth.filter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;
import ru.tasklist.springboot.auth.exception.JsonException;

import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;


// Перехватывает ошибки все фильтров, которые выполняется после текущего.
// Оборачивает ошибки в формат JSON и отправляет клиенту.
@Component
public class ExceptionHandlerFilter extends OncePerRequestFilter {

    @Override
    public void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain) throws ServletException, IOException {
        try {
            filterChain.doFilter(request, response); // вызов следующего по цепочке фильтра (если возникает ошибка - она обработается здесь)
        } catch (RuntimeException e) {

            // создать JSON и отправить название класса ошибки (также делали и в контроллере)
            JsonException ex = new JsonException(e.getClass().getSimpleName());

            response.setStatus(HttpStatus.UNAUTHORIZED.value()); // статус: не авторизован для данного действия
            response.getWriter().write(convertObjectToJson(ex)); // в ответе записываем JSON с классом ошибки
        }
    }


    // метод для преобразования объекта в JSON
    private String convertObjectToJson(Object object) throws JsonProcessingException {
        if (object == null) {
            return null;
        }
        ObjectMapper mapper = new ObjectMapper(); // объект из библиотеки jackson (ею пользуется и сам Spring), для формирования JSON
        return mapper.writeValueAsString(object); // формирует json
    }

}