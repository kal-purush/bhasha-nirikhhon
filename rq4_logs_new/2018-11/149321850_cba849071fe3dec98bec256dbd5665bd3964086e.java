package hu.uni.miskolc.iit.model;

/*
Maybe not the best name choice. This class contains the actor (User) and their action on the Form
Used in the History class.
 */

import java.util.Calendar;
import java.util.Date;

public class Action {
    private User user;
    private FormAction formAction;
    private Date date;

    public Action(User user, FormAction formAction){
        this.user = user;
        this.formAction = formAction;
        date = new Date();
        date = Calendar.getInstance().getTime();
    }

    public User getUser() {
        return user;
    }

    public void setUser(User user) {
        this.user = user;
    }

    public FormAction getFormAction() {
        return formAction;
    }

    public void setFormAction(FormAction formAction) {
        this.formAction = formAction;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    @Override
    public String toString(){
        return user.toString()+"\n"+ formAction.toString()+" the request on "+date;
    }
}