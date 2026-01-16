package io.github.mat3e.todo;

import com.fasterxml.jackson.databind.ObjectMapper;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

@WebServlet(name="todo", urlPatterns = {"api/todos/*"})
public class TodoServlet extends HttpServlet {

    private TodoRepository todoRepository;
    private ObjectMapper mapper;

    /**
     * Constructor used by Hibernate
     */
    @SuppressWarnings("unused")
    public TodoServlet(){
        this(new TodoRepository(), new ObjectMapper());
    }

    TodoServlet(TodoRepository todoRepository, ObjectMapper mapper){
        this.todoRepository = todoRepository;
        this.mapper = mapper;
    }

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws ServletException, IOException {
        resp.setContentType("application/json");
        mapper.writeValue(resp.getOutputStream(), todoRepository.findAll());
    }
}