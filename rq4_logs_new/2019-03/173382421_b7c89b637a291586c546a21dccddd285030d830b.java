package data;

import data.interfaces.IQuestionComponent;
import data.interfaces.IQuestionData;

public class QuestionComponent implements IQuestionComponent {

    private String type;
    private String name;
    private IQuestionData<?> questionData;

    // region Constructors

    /**
     * Create a Question Component with a custom Name and data
     *
     * @param type         specify Type of Question Component (like Title)
     * @param name         Name of Component, if there are more then one
     * @param questionData Data from this Component
     */
    public QuestionComponent(String type, String name, IQuestionData<?> questionData) {
        this.type = type;
        this.name = name;
        this.questionData = questionData;
    }

    /**
     * Create a Question Component with default name and data
     *
     * @param type         specify Type of Question Component (like Title)
     * @param questionData Data from this Component
     */
    public QuestionComponent(String type, IQuestionData<?> questionData) {
        this(type, type, questionData);
    }

    /**
     * Create an empty Question Component with custom name
     *
     * @param type specify Type of Question Component (like Title)
     * @param name Name of Component, if there are more then one
     */
    public QuestionComponent(String type, String name) {
        this(type, name, null);
    }

    /**
     * Create an empty Question Component with default name
     *
     * @param type specify Type of Question Component (like Title)
     */
    public QuestionComponent(String type) {
        this(type, type, null);
    }

    // endregion

    /**
     * get Component Type. Dependent on this type the GUI Component will be created.
     *
     * @return Component Type
     */
    @Override
    public String getType() {
        return type;
    }

    /**
     * Name of Component to identify Data for this Component in case there are more then one of these Components
     *
     * @return Name of Component
     */
    @Override
    public String getName() {
        return name;
    }

    /**
     * set the Component Name if there are more then one on one Question Layout
     *
     * @param name Name of the Component
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * get the specified Content Data for this Component
     *
     * @return Component Data
     */
    @Override
    public IQuestionData<?> getComponentData() {
        return questionData;
    }

    /**
     * Init Component and load Data from the Game.xml file
     *
     * @param data QuestionData for this Question from Game Data File
     */
    @Override
    public void setComponentData(IQuestionData<?> data) {
        questionData = data;
    }
}