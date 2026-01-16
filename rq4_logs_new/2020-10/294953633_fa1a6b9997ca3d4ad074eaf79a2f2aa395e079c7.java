package jimmy.mcgymmy.logic.predicate;

import java.util.function.Predicate;
import java.util.logging.Logger;

import jimmy.mcgymmy.commons.core.LogsCenter;
import jimmy.mcgymmy.logic.parser.ParserUtil;
import jimmy.mcgymmy.logic.parser.exceptions.ParseException;
import jimmy.mcgymmy.model.date.Date;
import jimmy.mcgymmy.model.food.Food;

public class DatePredicate implements Predicate<Food> {
    private final Date date;
    private final Logger logger = LogsCenter.getLogger(getClass());

    public DatePredicate(String date) {
        Date date1 = null;
        try {
            Date parsedDate = ParserUtil.parseDate(date);
            date1 = parsedDate;
        } catch (ParseException e) {
            logger.info("Invalid date: " + date);
        }
        this.date = date1;
    }

    @Override
    public boolean test(Food food) {
        return this.date == food.getDate();
    }
}