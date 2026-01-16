package ChapterEight;

import ChapterEight.TimeValidator;

public class TimeValidatorMain {
    public static void main(String[] args) {
        TimeValidator timeValidator = new TimeValidator(24,7,8);
       timeValidator.setSecond(45);
       timeValidator.setMinute(24);
       timeValidator.setHour(24);
       timeValidator.displayTime();
    }
}