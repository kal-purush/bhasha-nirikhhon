package notebook.util;

import notebook.model.User;

public class UserValidator {
    public User validate(User user) {
//        if (user.getFirstName().isEmpty()) throw new IllegalStateException
//                ("Имя не должно быть пустым!");
//        if (user.getLastName().isEmpty()) throw new IllegalStateException
//                ("Фамилия не должна быть пустым!");
//        if (user.getPhone().isEmpty()) throw new IllegalStateException
//                ("Номер телефона не должен быть пустым!");
        // или
        if (!isValid(user)) {
            throw new IllegalStateException
                ("Имя не должно быть пустым!");
        }
        user.setFirstName(user.getFirstName().replaceAll(" ", "").trim());
        user.setLastName(user.getLastName().replaceAll(" ", "").trim());
        user.setPhone(user.getPhone().replaceAll(" ", "").trim());
        return user;

    }

    private boolean isValid(User user) {
        return !user.getFirstName().isEmpty()
                || !user.getLastName().isEmpty()
                || !user.getPhone().isEmpty();
    }
}
