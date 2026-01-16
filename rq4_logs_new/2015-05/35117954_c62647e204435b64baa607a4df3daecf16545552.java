package puzzles;

import java.time.LocalDateTime;

/**
 * Puzzle #1: Birthday
 *
 * I came up with this puzzle as I was preparing material to use for interviewing new graduates. I didn't think that it
 * was a hard problem, but surprisingly 70% of the people I interviewed couldn't come up with an answer at all, so to
 * see if it was a fluke, I showed it to my coworkers. My colleagues solved it, but not as quickly as I thought they
 * would, and trying to solve the problem became a bit of a sensation for a couple days.
 *
 * The included method is simple - given a birthday, the method should return the age (in years) of a person born on
 * that day. The method could conceivably be used as:
 *    System.out.println("The user is " + computeAge(user.getBirthday()) + " years old.");
 *
 * Sometimes the method returns the correct age, sometimes it doesn't. Good luck!
 */

@SuppressWarnings("unused")
public class Puzzle1 {
  public static Integer computeAge(LocalDateTime birthday) throws IllegalArgumentException {
    if(birthday.isAfter(LocalDateTime.now())) {
      throw new IllegalArgumentException("Not born yet!");
    }
    return LocalDateTime.now().getYear() - birthday.getYear();
  }
}