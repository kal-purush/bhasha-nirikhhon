package models;

import utils.Utils;
import exceptions.ValidationException;

import java.time.LocalDateTime;

public class User {
    private String id, login, password, confirmPassword, lastName, firstName, middleName;
    private LocalDateTime createdAt;
    private int age;
    private boolean isWorker;

    public User() {
    }

    public User(String id, LocalDateTime createdAt, String login, String password, String confirmPassword, String lastName, String firstName, String middleName, int age, boolean isWorker) {
        setId(id);
        setCreatedAt(createdAt);
        setLogin(login);
        setPasswords(password, confirmPassword);
        setLastName(lastName);
        setFirstName(firstName);
        setMiddleName(middleName);
        setAge(age);
        setWorker(isWorker);
    }

    // TODO:
    //  я не очень понял насчет условий:
    //  "isWorker – является ли сотрудником предприятия, по умолчанию false" и
    //  "дата LocalDateTime добавления в систему, по умолчанию сегодня,формат: дата и время"
    //  звучит так, будто нужно создавать конструкторы без этих полей? то есть конструктор с isWorker,
    //  но без даты, потом наоборот, а также конструктор без обоих параметров, верно?
    //  или я чересчур заморачиваюсь? :)

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public void setCreatedAt() {
        createdAt = LocalDateTime.now();
    }

    public String getLogin() {
        return login;
    }

    public void setLogin(String login) {
        // "^(?=.*[a-zA-Z])(?=.*\\d+)(?=.*_).{1,20}$")
        if (
                login == null
                        || login.length() > 20
                        || !Utils.containsLetters(login)
                        || !Utils.containsDigits(login)
                        || !Utils.containsSymbol(login, '_')
        ) {
            throw new ValidationException("Логин должен содержать буквы, цифры, знак подчеркивания, меньше 20 символов. Передано значение: " + login);
        }
        this.login = login;
    }

    public void setPasswords(String password, String confirmPassword) {
        validatePassword(password);
        validatePassword(confirmPassword);
        if (!password.equals(confirmPassword)) {
            throw new ValidationException("Пароли не совпадают");
        }
        this.password = password;
        this.confirmPassword = confirmPassword;
    }

    private void validatePassword(String password) {
        if (
                password == null
                        || password.length() > 20
                        || !Utils.containsLetters(password)
                        || !Utils.containsDigits(password)
                        || !Utils.containsSymbol(password, '_')
        ) {
            throw new ValidationException("Пароль должен содержать буквы, цифры, знак подчеркивания, меньше 20 символов");
            // пароли даже неправильные не печатаем в консоль из соображений безопасности
        }
    }

    public String getPassword() {
        return password;
    }

    public String getConfirmPassword() {
        return confirmPassword;
    }

    public String getLastName() {
        return lastName;
    }

    public void setLastName(String lastName) {
        if (lastName == null || !Utils.containsLettersOnly(lastName)) {
            throw new ValidationException("Фамилия должна состоять только из букв. Передано значение: " + lastName);
        }
        this.lastName = lastName;
    }

    public String getFirstName() {
        return firstName;
    }

    public void setFirstName(String firstName) {
        if (firstName == null || !Utils.containsLettersOnly(firstName)) {
            throw new ValidationException("Имя должно состоять только из букв. Передано значение: " + firstName);
        }
        this.firstName = firstName;
    }

    public String getMiddleName() {
        return middleName;
    }

    public void setMiddleName(String middleName) {
        if (middleName != null && !Utils.containsLettersOnly(middleName)) {
            throw new ValidationException("Отчество должно состоять только из букв. Передано значение: " + middleName);
        }
        this.middleName = middleName;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public boolean isWorker() {
        return isWorker;
    }

    public void setWorker(boolean worker) {
        isWorker = worker;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        // в условиях явно не сказано, что id должен быть в формате UUID, поэтому проверяем, что содержит цифры и буквы
        if (id == null || !Utils.containsDigits(id) || !Utils.containsLetters(id)) {
            throw new ValidationException("id должен содержать цифры и буквы. Передано значение: " + id);
        }
        this.id = id;
    }

    @Override
    public String toString() {
        return "User{" +
                "id='" + id + '\'' +
                ", createdAt=" + createdAt +
                ", login='" + login + '\'' +
                ", password='" + password + '\'' +
                ", confirmPassword='" + confirmPassword + '\'' +
                ", lastName='" + lastName + '\'' +
                ", firstName='" + firstName + '\'' +
                ", middleName='" + middleName + '\'' +
                ", age=" + age +
                ", isWorker=" + isWorker +
                '}';
    }
}