package ua.ithillel.bank.dataJpa;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.List;
import java.util.UUID;

import static ua.ithillel.bank.dataJpa.UserRole.ADMIN;
import static ua.ithillel.bank.dataJpa.UserRole.CUSTOMER;

@SpringBootApplication
public class SpringDataApplication {
    public static void main(String[] args) {
        ConfigurableApplicationContext context = SpringApplication.run(SpringDataApplication.class, args);
        UserService userService = context.getBean(UserService.class);

         // --- вводим новых пользователей
        userService.save(User.builder().uid(UUID.randomUUID().toString()).name("Tim Burton").email("burton@gmail.com").userRole(ADMIN).build());
        userService.save(User.builder().uid(UUID.randomUUID().toString()).name("Rob Snider").email("snider1968@yahoo.us").userRole(CUSTOMER).build());
        userService.save(User.builder().uid(UUID.randomUUID().toString()).name("Kate Perry").email("k_perry@gmail.com").userRole(CUSTOMER).build());

         // --- Update
        Long id = 4L;
        User userN = userService.getById(id);               // достанем объект пользователя по ID
        userN.setEmail("dublin@mail.ua");                   // поменяем ему в объекте email
        userService.update(id, userN);                      // обновим, передав пользователя с новым email
        userService.getById(id);                            // убедимся, что email обновлен

        userService.delete(id);                             // удалим пользователя

        // --- вывод списка из таблицы "users"
        List<User> userList = userService.getAllUsers();
        for (User user : userList) {
            System.out.println(user);
        }

    }
}