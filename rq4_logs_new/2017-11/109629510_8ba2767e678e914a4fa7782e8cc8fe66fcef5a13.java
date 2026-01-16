package com.bsuir.archive.server.controller.command.implamentation;

import com.bsuir.archive.server.auxiliary.manager.UserManager;
import com.bsuir.archive.server.controller.command.Command;
import com.bsuir.archive.server.domain.User;
import com.bsuir.archive.server.service.ServiceFactory;
import com.bsuir.archive.server.service.UserService;
import com.bsuir.archive.server.service.exception.ServiceException;

public class AddUserCommand implements Command {


    UserService userService;

    public AddUserCommand() {
        ServiceFactory factory = ServiceFactory.getInstance();
        userService = factory.getUserService();
    }

    @Override
    public String execute(String[] param) {
        String response = "Пользователь создан";
        Boolean result = true;
        User user = new User(param[1],param[2],parseBool(param[3]),parseBool(param[4]),parseBool(param[5]),parseBool(param[6]));
        try {
            result = userService.addUser(user);
        }catch (ServiceException ex){
                //TODO
            response = "Произошла ошибка";
            result = false;
        }
        if (!result){
            response = "Пользователь не добавлен";
        }
        return response;
    }

    Boolean parseBool(String str){
        Boolean result;
        try{
            result = Boolean.parseBoolean(str);
        }catch (Exception ex){
            result = false;
        }
        return result;
    }
    @Override
    public Boolean isAccessSee() {
        return AccessSee;
    }

    @Override
    public Boolean isAccessWrite() {
        return AccessWrite;
    }

    @Override
    public Boolean isAccessChange() {
        return AccessChange;
    }

    @Override
    public String getDescriptionCommand() {
        return description;
    }

    @Override
    public Integer getCountParam() {
        return countParam;
    }

    @Override
    public Boolean isAccessAdmin() {
        return AccessAdmin;
    }


    private static final int countParam = 7;
    private String description = "Adding a user: adduser|login|password|accsessSee|accessWrite|accessChane|accessAdmin";
    Boolean AccessSee = false;
    Boolean AccessWrite = true;
    Boolean AccessChange = true;
    Boolean AccessAdmin = true;
}