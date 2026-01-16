package jimmy.mcgymmy.logic.commands;

import org.junit.jupiter.api.Test;

import jimmy.mcgymmy.commons.core.Messages;
import jimmy.mcgymmy.commons.core.index.Index;
import jimmy.mcgymmy.logic.parser.CommandParserTestUtil;
import jimmy.mcgymmy.model.Model;
import jimmy.mcgymmy.model.ModelManager;
import jimmy.mcgymmy.model.UserPrefs;
import jimmy.mcgymmy.model.food.Food;
import jimmy.mcgymmy.model.tag.Tag;
import jimmy.mcgymmy.testutil.FoodBuilder;
import jimmy.mcgymmy.testutil.TypicalFoods;
import jimmy.mcgymmy.testutil.TypicalIndexes;

class TagCommandTest {

    private Model model = new ModelManager(TypicalFoods.getTypicalMcGymmy(), new UserPrefs());

    @Test
    public void execute_validIndexUnfilteredList_success() {
        Food foodToTag = model.getFilteredFoodList().get(TypicalIndexes.INDEX_FIRST_FOOD.getZeroBased());
        TagCommand tagCommand = new TagCommand();
        tagCommand.setParameters(
                new CommandParserTestUtil.ParameterStub<>("", TypicalIndexes.INDEX_FIRST_FOOD),
                new CommandParserTestUtil.ParameterStub<>("t", new Tag("hello"))
        );

        String expectedMessage = String.format(TagCommand.MESSAGE_SUCCESS, "hello");

        ModelManager expectedModel = new ModelManager(model.getMcGymmy(), new UserPrefs());
        expectedModel.setFood(foodToTag, new FoodBuilder(foodToTag).withTags("hello").build());

        CommandTestUtil.assertCommandSuccess(tagCommand, model, expectedMessage, expectedModel);
    }

    @Test
    public void execute_invalidIndexUnfilteredList_throwsCommandException() {
        Index outOfBoundIndex = Index.fromOneBased(model.getFilteredFoodList().size() + 1);
        TagCommand tagCommand = new TagCommand();
        tagCommand.setParameters(
                new CommandParserTestUtil.ParameterStub<>("", outOfBoundIndex),
                new CommandParserTestUtil.ParameterStub<>("t", new Tag("hello"))
        );
        CommandTestUtil.assertCommandFailure(tagCommand, model, Messages.MESSAGE_INVALID_FOOD_DISPLAYED_INDEX);
    }

    @Test
    public void execute_validIndexFilteredList_success() {

        CommandTestUtil.showFoodAtIndex(model, TypicalIndexes.INDEX_FIRST_FOOD);

        Food foodToTag = model.getFilteredFoodList().get(TypicalIndexes.INDEX_FIRST_FOOD.getZeroBased());
        TagCommand tagCommand = new TagCommand();
        tagCommand.setParameters(
                new CommandParserTestUtil.ParameterStub<>("", TypicalIndexes.INDEX_FIRST_FOOD),
                new CommandParserTestUtil.ParameterStub<>("t", new Tag("goodbye"))
        );

        String expectedMessage = String.format(TagCommand.MESSAGE_SUCCESS, "goodbye");
        ModelManager expectedModel = new ModelManager(model.getMcGymmy(), new UserPrefs());
        expectedModel.setFood(model.getFilteredFoodList().get(0),
                new FoodBuilder(foodToTag).withTags("goodbye").build());

        CommandTestUtil.assertCommandSuccess(tagCommand, model, expectedMessage, expectedModel);
    }

}