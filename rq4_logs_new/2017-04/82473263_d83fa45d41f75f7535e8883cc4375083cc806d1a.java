package techpark.service;


import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;

import org.springframework.transaction.annotation.Transactional;
import techpark.DAO.RequestUsersDAO;
import techpark.exceptions.AccountServiceDBException;
import techpark.exceptions.AccountServiceDDException;
import techpark.user.UserProfile;
import techpark.user.UserToInfo;


/**
 * Created by Fedorova on 20/02/2017.
 */

@SuppressWarnings("ThrowInsideCatchBlockWhichIgnoresCaughtException")
@Transactional
@Service
public class AccountServiceImpl implements AccountService{

    private final RequestUsersDAO usersDAO;

    public AccountServiceImpl(JdbcTemplate jdbcTemplate) {
        this.usersDAO = new RequestUsersDAO(jdbcTemplate);
    }

    @Override
    public void register(@NotNull String mail, @NotNull String login, @NotNull String password)
            throws AccountServiceDBException, AccountServiceDDException {
        try {
            usersDAO.addUser(mail, login, password);
        } catch (DuplicateKeyException k) {
            throw new AccountServiceDDException("Duplicate value");
        } catch (DataAccessException a) {
            throw new AccountServiceDBException("Database error");
        }
    }

    @Override
    public boolean verifyMail(@NotNull String mail)
            throws AccountServiceDBException {
        try {
            usersDAO.verifyMail(mail);
            return true;
        } catch (EmptyResultDataAccessException e) {
            //throw new AccountServiceException("Empty result", 0);
            return false;
        } catch (DataAccessException a) {
            throw new AccountServiceDBException("Database error");
        }
    }

    @Override
    @Nullable
    public UserProfile getUserByLogin(@NotNull String login)
            throws AccountServiceDBException {
        try {
            return usersDAO.getUserByLogin(login);
        } catch (EmptyResultDataAccessException e) {
            return null;
        } catch (DataAccessException a) {
            throw new AccountServiceDBException("Database error");
        }
    }

    @Override
    public void changeUser(@NotNull UserProfile user, @Nullable String login, @Nullable String mail,
                           @Nullable String password) throws AccountServiceDBException, AccountServiceDDException {

        try {
            usersDAO.changeUser(user, login, mail, password);
        } catch (DuplicateKeyException k) {
            throw new AccountServiceDDException("Duplicate value");
        } catch (DataAccessException a) {
            throw new AccountServiceDBException("Database error");
        }
    }

    @Override
    public void changeScore(@NotNull UserProfile currentUser, @NotNull Integer score)
            throws AccountServiceDBException {
        try {
            if (currentUser.getScore() < score)
                usersDAO.changeScore(score, currentUser.getLogin());
        } catch (DataAccessException a) {
            throw new AccountServiceDBException("Database error");
        }
    }

    @Override
    public int getScore(@NotNull String login) throws AccountServiceDBException {
        try {
            return usersDAO.getScore(login);
        } catch (DataAccessException a) {
            throw new AccountServiceDBException("Database error");
        }
    }

    @Override
    @NotNull
    public ArrayList<UserToInfo> getAllUsers() throws AccountServiceDBException {
        try {
            return usersDAO.getBestUsers();
        } catch (DataAccessException a) {
            throw new AccountServiceDBException("Database error");
        }

    }

}