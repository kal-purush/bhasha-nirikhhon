package by.mordas.project.dto;

import by.mordas.project.entity.Subject;
import by.mordas.project.entity.User;

import java.sql.Date;
import java.util.Map;

/***
 Author: Sergei Mordas
 Date: 27.05.2018
 ***/
public class UserDTO {

    /**This is the user*/
    private User user;

    /**This is summary user scores*/
    private int sumScores;

    /**This is user status*/
    private boolean accepted;

    /**
     * Instantiates a new speciality dto.
     */
    public UserDTO(){

    }

    /**
     * Instantiates a new speciality dto.
     *
     * @param user the user
     */
    public UserDTO(User user) {
        this.user = user;
    }

    /**
     * Instantiates a new speciality dto.
     *
     * @param user the user
     * @param sumScores the user summary scores
     * @param accepted the user status
     */
    public UserDTO(User user, int sumScores, boolean accepted) {
        this.user = user;
        this.sumScores = sumScores;
        this.accepted = accepted;
    }

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }

    public int getSumScores() {
        return sumScores;
    }

    public void setSumScores(int sumScores) {
        this.sumScores = sumScores;
    }

    public boolean isAccepted() {
        return accepted;
    }

    public void setAccepted(boolean accepted) {
        this.accepted = accepted;
    }

    public long getUserId() {
        return user.getUserId();
    }

    public String getLogin() {
        return user.getLogin();
    }

    public String getFirstName() {
        return user.getFirstName();
    }

    public String getLastName() {
        return user.getLastName();
    }

    public int getRoleId() {
        return user.getRoleId();
    }

    public String getEmail() {
        return user.getEmail();
    }

    public Map<Subject, Integer> getSubjectMark() {
        return user.getSubjectMark();
    }

    public String getPassword() {
        return user.getPassword();
    }

    public int getCertificateMark() {
        return user.getCertificateMark();
    }

    public Date getBirthday() {
        return user.getBirthday();
    }

    public long getSpecialityId() {
        return user.getSpecialityId();
    }

    public void setLogin(String login) {
        user.setLogin(login);
    }

    public void setPassword(String password) {
        user.setPassword(password);
    }

    public void setRoleId(int roleId) {
        user.setRoleId(roleId);
    }

    public void setEmail(String email) {
        user.setEmail(email);
    }

    public void setSubjectMark(Map<Subject, Integer> subjectMark) {
        user.setSubjectMark(subjectMark);
    }

    public void setUserId(long userId) {
        user.setUserId(userId);
    }

    public void setFirstName(String firstName) {
        user.setFirstName(firstName);
    }

    public void setLastName(String lastName) {
        user.setLastName(lastName);
    }

    public void setCertificateMark(int certificateMark) {
        user.setCertificateMark(certificateMark);
    }

    public void setSpecialityId(long specialityId) {
        user.setSpecialityId(specialityId);
    }

    public void setBirthday(Date birthday) {
        user.setBirthday(birthday);
    }
}