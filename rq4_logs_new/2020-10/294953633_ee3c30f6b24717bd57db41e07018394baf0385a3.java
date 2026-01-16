package jimmy.mcgymmy.logic.commands;

import static java.util.Objects.requireNonNull;

import java.util.List;

import jimmy.mcgymmy.commons.core.Messages;
import jimmy.mcgymmy.commons.core.index.Index;
import jimmy.mcgymmy.logic.commands.exceptions.CommandException;
import jimmy.mcgymmy.logic.parser.ParserUtil;
import jimmy.mcgymmy.logic.parser.parameter.Parameter;
import jimmy.mcgymmy.model.Model;
import jimmy.mcgymmy.model.food.Food;
import jimmy.mcgymmy.model.tag.Tag;

public class UnTagCommand extends Command {
    public static final String COMMAND_WORD = "untag";

    public static final String MESSAGE_SUCCESS = "Tag removed: %s";
    public static final String MESSAGE_NOT_FOUND_TAG = "Tag %s does not exist exists in %s";

    private Parameter<Index> indexParameter = this.addParameter(
            "index",
            "",
            "Index of selected Food",
            "1",
            ParserUtil::parseIndex
    );

    private Parameter<Tag> tagParameter = this.addParameter(
            "Tag",
            "t",
            "Tag to be removed from food",
            "Lunch",
            ParserUtil::parseTag
    );

    @Override
    public CommandResult execute(Model model) throws CommandException {
        requireNonNull(model);
        List<Food> lastShownList = model.getFilteredFoodList();
        Index index = indexParameter.consume();

        if (index.getZeroBased() >= lastShownList.size()) {
            throw new CommandException(Messages.MESSAGE_INVALID_FOOD_DISPLAYED_INDEX);
        }

        Food foodToTag = lastShownList.get(index.getZeroBased());
        Tag tag = tagParameter.consume();

        if (!foodToTag.getTags().contains(tag)) {
            throw new CommandException(String.format(MESSAGE_NOT_FOUND_TAG, tag.tagName, foodToTag.getName().fullName));
        }

        foodToTag.removeTag(tag);
        model.setFood(foodToTag, foodToTag); //To refresh the card
        model.updateFilteredFoodList(Model.PREDICATE_SHOW_ALL_FOODS);
        return new CommandResult(String.format(MESSAGE_SUCCESS, tag.tagName));
    }
}