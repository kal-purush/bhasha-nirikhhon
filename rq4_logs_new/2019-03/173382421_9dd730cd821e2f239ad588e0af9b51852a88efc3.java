package Service;

import data.interfaces.IQuestionLayout;

import java.util.ArrayList;
import java.util.List;

/**
 * Provides all defined Question Layouts.
 * Every Game can have its own Layouts, therefor every Game needs its own QuestionLayoutProvider.
 */
public class QuestionLayoutProvider {

    private List<IQuestionLayout> Layouts;

    // region Constructors

    /**
     * Creates a new QuestionLayoutProvider with given Layouts
     *
     * @param layouts Layout, which will be added to the Layout List
     */
    public QuestionLayoutProvider(List<IQuestionLayout> layouts) {
        this.Layouts = new ArrayList<>();
        if (layouts != null) {
            this.Layouts.addAll(layouts);
        }
    }

    /**
     * Create empty QuestionLayoutProvider
     */
    public QuestionLayoutProvider() {
        this(null);
    }

    // endregion

    /**
     * Get a copy of a Layout to set it to an Question
     *
     * @param name Name of the Layout
     * @return Copy of the Layout
     */
    public IQuestionLayout GetLayout(String name) {
        //TODO Create Copy of Layout and return it
        throw new UnsupportedOperationException();
    }

    /**
     * Clears all Layouts and add the new ones.
     *
     * @param layouts New Game Layouts
     */
    public void InitLayouts(List<IQuestionLayout> layouts) {
        this.Layouts.clear();
        this.Layouts.addAll(layouts);
    }

    /**
     * add a new Layout to the Layout List.
     *
     * @param layout Layout to be added
     */
    public void AddLayout(IQuestionLayout layout) {
        this.Layouts.add(layout);
    }

}