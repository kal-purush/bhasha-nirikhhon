package jimmy.mcgymmy.storage;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import jimmy.mcgymmy.commons.exceptions.IllegalValueException;
import jimmy.mcgymmy.model.food.Fat;
import jimmy.mcgymmy.model.food.Food;
import jimmy.mcgymmy.model.food.Protein;
import jimmy.mcgymmy.model.food.Carbohydrate;
import jimmy.mcgymmy.model.tag.Tag;

/**
 * Jackson-friendly version of {@link Food}.
 */
class JsonAdaptedFood {

    public static final String MISSING_FIELD_MESSAGE_FORMAT = "Food's %s field is missing!";

    private final String name;
    private final String protein;
    private final String fat;
    private final String carbs;
    private final List<JsonAdaptedTag> tagged = new ArrayList<>();

    /**
     * Constructs a {@code JsonAdaptedFood} with the given food details.
     */
    @JsonCreator
    public JsonAdaptedFood(@JsonProperty("name") String name, @JsonProperty("protein") String protein,
                           @JsonProperty("fat") String fat, @JsonProperty("carbs") String carbs,
                           @JsonProperty("tagged") List<JsonAdaptedTag> tagged) {
        this.name = name;
        this.protein = protein;
        this.fat = fat;
        this.carbs = carbs;
        if (tagged != null) {
            this.tagged.addAll(tagged);
        }
    }

    /**
     * Converts a given {@code Food} into this class for Jackson use.
     */
    public JsonAdaptedFood(Food source) {
        name = source.getName();
        protein = Integer.toString(source.getProtein().getAmount());
        fat = Integer.toString(source.getFat().getAmount());
        carbs = Integer.toString(source.getCarbs().getAmount());
        tagged.addAll(source.getTags().stream()
            .map(JsonAdaptedTag::new)
            .collect(Collectors.toList()));
    }

    /**
     * Converts this Jackson-friendly adapted food object into the model's {@code Food} object.
     *
     * @throws IllegalValueException if there were any data constraints violated in the adapted food.
     */
    public Food toModelType() throws IllegalValueException {
        final List<Tag> personTags = new ArrayList<>();
        for (JsonAdaptedTag tag : tagged) {
            personTags.add(tag.toModelType());
        }

        //        if (name == null) {
        //            throw new IllegalValueException(String.format(MISSING_FIELD_MESSAGE_FORMAT, Name.class.getSimpleName()));
        //        }
        //        if (!Name.isValidName(name)) {
        //            throw new IllegalValueException(Name.MESSAGE_CONSTRAINTS);
        //        }
        final String modelName = name;

        if (protein == null) {
            throw new IllegalValueException(String.format(MISSING_FIELD_MESSAGE_FORMAT, Protein.class.getSimpleName()));
        }
        if (!Protein.isValid(protein)) {
            throw new IllegalValueException(Protein.MESSAGE_CONSTRAINTS);
        }
        final Protein modelProtein = new Protein(Integer.parseInt(protein));

        if (fat == null) {
            throw new IllegalValueException(String.format(MISSING_FIELD_MESSAGE_FORMAT, Fat.class.getSimpleName()));
        }
        if (!Fat.isValid(fat)) {
            throw new IllegalValueException(Fat.MESSAGE_CONSTRAINTS);
        }
        final Fat modelFat = new Fat(Integer.parseInt(fat));

        if (carbs == null) {
            throw new IllegalValueException(String.format(MISSING_FIELD_MESSAGE_FORMAT, Carbohydrate.class.getSimpleName()));
        }
        if (!Carbohydrate.isValid(carbs)) {
            throw new IllegalValueException(Carbohydrate.MESSAGE_CONSTRAINTS);
        }
        final Carbohydrate modelCarbohydrate = new Carbohydrate(Integer.parseInt(carbs));

        final Set<Tag> modelTags = new HashSet<>(personTags);
        return new Food(modelName, modelProtein, modelFat, modelCarbohydrate, modelTags);
    }

}