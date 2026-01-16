package ru.clean.process.api.dto.user;

import java.util.List;

/**
 * Base User interface implementation
 */
public class UserImpl implements User{
    protected Long id;
    protected String name;
    protected String secondName;
    protected String login;
    protected String password;
    protected List<UserRoles> userRoles;

    @Override
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    @Override
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String getSecondName() {
        return secondName;
    }

    public void setSecondName(String secondName) {
        this.secondName = secondName;
    }

    @Override
    public String getLogin() {
        return login;
    }

    public void setLogin(String login) {
        this.login = login;
    }

    @Override
    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public List<UserRoles> getRoles() {
        return userRoles;
    }

    public void setRoles(List<UserRoles> userRoles) {
        this.userRoles = userRoles;
    }

    public UserImpl() {
    }

    public UserImpl(User user) {
        this.id = user.getId();
        this.name = user.getName();
        this.secondName = user.getSecondName();
        this.login = user.getLogin();
        this.password = user.getPassword();
        this.userRoles = user.getRoles();
    }
}