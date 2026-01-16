/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package beans;

import java.util.Date;
import java.util.List;
import javax.faces.bean.ManagedBean;
import javax.faces.bean.ManagedProperty;
import javax.faces.bean.RequestScoped;
import model.Answers;
import user.service.AnswerService;
import userservice.Interfaces.IAnswerService;

@ManagedBean
@RequestScoped
public class AnswerBean {

    @ManagedProperty(value = "#{userBean}")
    private UserBean userBean;
    private String answer;
    private Date date;
    private String Email;

    private static int question_id;

    IAnswerService answerService;

    public String addAnswer(int id) {
        Answers ans = new Answers();
        ans.setAnswerText(answer);
        ans.setDate(new Date());
        ans.setEmail(userBean.getEmail());
        ans.setQuesionId(getQuestion_id());
        getAnswerService().addQuestion(ans);

        return "Show";
    }

    public List<Answers> getAllAnswers(int id) {
        return getAnswerService().getAllAnswers(id);

    }

    public String getAnswer() {
        return answer;
    }

    public void setAnswer(String answer) {
        this.answer = answer;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    public UserBean getUserBean() {
        return userBean;
    }

    public void setUserBean(UserBean userBean) {
        this.userBean = userBean;
    }

    public String getEmail() {
        return Email;
    }

    public void setEmail(String Email) {
        this.Email = Email;
    }

    public static int getQuestion_id() {
        return question_id;
    }

    public static void setQuestion_id(int question_id) {
        AnswerBean.question_id = question_id;
    }

    public IAnswerService getAnswerService() {
        if (answerService == null) {
            answerService = new AnswerService();
        }
        return answerService;
    }
}