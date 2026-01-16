package data;

import data.interfaces.ICategory;
import data.interfaces.IQuestion;

import java.util.List;

/**
 * Set of Questions within a Game with the same Category
 *
 * @author Johannes
 */
public class Category implements ICategory {

    private String Name;
    private List<IQuestion> Questions;

    /**
     * get Category Name
     *
     * @return Category Name
     */
    @Override
    public String getName() {
        return Name;
    }

    /**
     * Get all Questions of this Category
     *
     * @return List with all Questions
     */
    @Override
    public List<IQuestion> getQuestions() {
        return Questions;
    }

    /**
     * add a Question to this Category. Value of Questions must be unique!
     *
     * @param question Question that will be added
     */
    @Override
    public void addQuestion(IQuestion question) {
        Questions.add(question);
    }

    /**
     * get a Question by value.
     *
     * @param value Value of Question
     * @return Question with specified Value
     */
    @Override
    public IQuestion getQuestions(int value) {
        //TODO implement
        throw new UnsupportedOperationException();
    }
}