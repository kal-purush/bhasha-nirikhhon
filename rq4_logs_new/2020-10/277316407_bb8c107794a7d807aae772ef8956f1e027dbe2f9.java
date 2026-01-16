package ru.otus.server;

import org.eclipse.jetty.security.AbstractLoginService;
import org.eclipse.jetty.util.security.Password;
import ru.otus.core.model.User;

public class ExtendedUserPrincipal extends AbstractLoginService.UserPrincipal {

    private final User userDbData;

    public ExtendedUserPrincipal(User userDbData) {
        super(userDbData.getLogin(), new Password(userDbData.getPassword()));
        this.userDbData = userDbData;
    }

    public User getUserDbData() {
        return userDbData;
    }

    public UserPublicData getpublicUData() {
        return new UserPublicData(userDbData);
    }

}
