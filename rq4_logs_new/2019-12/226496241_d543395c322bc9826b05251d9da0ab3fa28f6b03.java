package olympic;

import olympic.business.*;
import org.junit.Test;

import java.util.ArrayList;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static olympic.Solution.*;
import static olympic.business.ReturnValue.*;
import static org.junit.Assert.assertEquals;

public class GradingTests extends AbstractTest {
    /**
     * CRUD TEST
     */
    @Test
    public void AthleteTest() {
        Athlete a = new Athlete();
        a.setId(1);
        a.setName("Neymar");
        a.setCountry("Brazil");
        a.setIsActive(true);
        ReturnValue ret = addAthlete(a);
        assertEquals(OK, ret);

        ret = addAthlete(a);
        assertEquals(ALREADY_EXISTS, ret);

        a.setId(0);
        ret = addAthlete(a);
        assertEquals(BAD_PARAMS, ret);

        for (int i = 2; i < 20; i++) {
            a.setId(i);
            ret = addAthlete(a);
            assertEquals(OK, ret);
        }

        //got 1-19
        a = getAthleteProfile(0);
        assertEquals(Athlete.badAthlete(), a);
        a = getAthleteProfile(20);
        assertEquals(Athlete.badAthlete(), a);
        for (int i = 1; i < 20; i++) {
            a = getAthleteProfile(i);
            assertEquals(i, a.getId());
            assertEquals("Brazil", a.getCountry());
            assertEquals("Neymar", a.getName());
            assertEquals(true, a.getIsActive());
        }

        a.setId(20);
        assertEquals(NOT_EXISTS, deleteAthlete(a));
        a.setId(0);
        assertEquals(NOT_EXISTS, deleteAthlete(a));
        for (int i = 1; i < 20; i++) {
            a.setId(i);
            assertEquals(OK, deleteAthlete(a));
        }
    }

    @Test
    public void SportTest() {
        Sport a = new Sport();
        a.setId(1);
        a.setName("Football");
        a.setCity("Tokyo");
        a.setAthletesCount(5);
        ReturnValue ret = addSport(a);
        assertEquals(OK, ret);

        ret = addSport(a);
        assertEquals(ALREADY_EXISTS, ret);

        a.setId(0);
        ret = addSport(a);
        assertEquals(BAD_PARAMS, ret);

        for (int i = 2; i < 20; i++) {
            a.setId(i);
            if (i % 2 == 0)
                a.setAthletesCount(0);
            else
                a.setAthletesCount(i);
            ret = addSport(a);
            assertEquals(OK, ret);
        }

        //got 1-19
        a = getSport(0);
        assertEquals(Sport.badSport(), a);
        a = getSport(20);
        assertEquals(Sport.badSport(), a);
        for (int i = 1; i < 20; i++) {
            a = getSport(i);
            assertEquals(i, a.getId());
            assertEquals("Tokyo", a.getCity());
            assertEquals("Football", a.getName());
            assertEquals(0, a.getAthletesCount());
        }

        a.setId(20);
        assertEquals(NOT_EXISTS, deleteSport(a));
        a.setId(0);
        assertEquals(NOT_EXISTS, deleteSport(a));
        for (int i = 1; i < 20; i++) {
            a.setId(i);
            assertEquals(OK, deleteSport(a));
        }
        a.setAthletesCount(-1);
        a.setId(1111);
        assertEquals(OK,addSport(a));
    }

    @Test
    public void BasicInputErrors() {
        Athlete a = new Athlete();
        a.setId(1);
        a.setName(null);
        a.setIsActive(true);
        a.setCountry("");
        ReturnValue ret = addAthlete(a);
        assertEquals(BAD_PARAMS, ret);

        a.setName("");
        a.setCountry(null);
        ret = addAthlete(a);
        assertEquals(BAD_PARAMS, ret);

        a.setCountry("");
        a.setId(-1);
        ret = addAthlete(a);
        assertEquals(BAD_PARAMS, ret);

        Sport s = new Sport();
        s.setId(1);
        s.setCity("");
        s.setName(null);
        s.setAthletesCount(0);
        ret = addSport(s);
        assertEquals(BAD_PARAMS, ret);

        s.setName("");
        s.setCity(null);
        ret = addSport(s);
        assertEquals(BAD_PARAMS, ret);

        s.setCity("");
        s.setAthletesCount(5);
        ret = addSport(s);
        assertEquals(OK, ret);

        s = getSport(1);
        assertEquals(0, s.getAthletesCount());

    }

    /**
     * BASIC API
     */
    @Test
    public void athleteJoinLeaveSportTest() {
        Athlete a = new Athlete();
        a.setId(1);
        a.setName("Neymar");
        a.setCountry("Brazil");
        a.setIsActive(true);
        ReturnValue ret = addAthlete(a);
        assertEquals(OK, ret);

        Sport s = new Sport();
        s.setId(1);
        s.setName("Football");
        s.setCity("Tokyo");
        s.setAthletesCount(0);//actual 0
        ret = addSport(s);
        assertEquals(OK, ret);

        ret = athleteJoinSport(2, 1);
        assertEquals(NOT_EXISTS, ret);

        ret = athleteJoinSport(1, 2);
        assertEquals(NOT_EXISTS, ret);

        ret = athleteJoinSport(2, 2);
        assertEquals(NOT_EXISTS, ret);

        ret = athleteJoinSport(1, 1);
        assertEquals(OK, ret);

        ret = athleteJoinSport(1, 1);
        assertEquals(ALREADY_EXISTS, ret);

        a.setId(2);
        a.setName("Arthur");
        ret = addAthlete(a);
        assertEquals(OK, ret);

        ret = athleteJoinSport(1, 2);
        assertEquals(OK, ret);

        ret = athleteJoinSport(1, 2);
        assertEquals(ALREADY_EXISTS, ret);

        ret = athleteJoinSport(2, 2);
        assertEquals(NOT_EXISTS, ret);

        s.setId(2);
        s.setName("Basketball");
        ret = addSport(s);
        assertEquals(OK, ret);

        ret = athleteJoinSport(2, 2);
        assertEquals(OK, ret);

        s = getSport(1);
        assertEquals(2, s.getAthletesCount());

        s = getSport(2);
        assertEquals(1, s.getAthletesCount());

        /**
         * sport 1 joined sport 1,2
         * sport 2 joined sport 2
         */

        a.setId(3);
        a.setName("Maradona");
        a.setIsActive(false);
        a.setCountry("Argentina");

        ret = addAthlete(a);
        assertEquals(OK, ret);

        ret = athleteJoinSport(1, 3);
        assertEquals(OK, ret);

        s = getSport(1);
        assertEquals(2, s.getAthletesCount());

        ret = athleteLeftSport(1, 3);
        assertEquals(OK, ret);

        s = getSport(1);
        assertEquals(2, s.getAthletesCount());

        ret = athleteLeftSport(1, 3);
        assertEquals(NOT_EXISTS, ret);

        ret = athleteLeftSport(1, 4);
        assertEquals(NOT_EXISTS, ret);

        ret = athleteLeftSport(3, 3);
        assertEquals(NOT_EXISTS, ret);

        ret = athleteLeftSport(3, 4);
        assertEquals(NOT_EXISTS, ret);

        ret = athleteLeftSport(1, 1);
        assertEquals(OK, ret);

        s = getSport(1);
        assertEquals(1, s.getAthletesCount());

        ret = athleteLeftSport(1, 2);
        assertEquals(OK, ret);

        s = getSport(1);
        assertEquals(0, s.getAthletesCount());

        ret = athleteLeftSport(2, 2);
        assertEquals(OK, ret);

        s = getSport(2);
        assertEquals(0, s.getAthletesCount());

        Solution.clearTables();
        a.setId(1);
        addAthlete(a);
        s.setId(1);
        addSport(s);
        ret = athleteJoinSport(1, 1);
        assertEquals(OK, ret);

        deleteSport(s);
        ret = athleteJoinSport(1, 1);
        assertEquals(NOT_EXISTS, ret);

        addSport(s);
        athleteJoinSport(1, 1);
        deleteAthlete(a);

        ret = athleteJoinSport(1, 1);
        assertEquals(NOT_EXISTS, ret);

        s.setId(2);
        addSport(s);
        a.setIsActive(true);
        for (int i = 100; i < 115; i++) {
            a.setId(i);
            addAthlete(a);
            athleteJoinSport(2, i);
            s = getSport(2);
            assertEquals(i - 99, s.getAthletesCount());
        }
        for (int i = 114; i > 99; i--) {
            athleteLeftSport(2, i);
            s = getSport(2);
            assertEquals(i - 100, s.getAthletesCount());
        }
    }

    @Test
    public void StandingTest() {
        Athlete a = new Athlete();
        a.setId(1);
        a.setName("Neymar");
        a.setCountry("Brazil");
        a.setIsActive(true);
        ReturnValue ret = addAthlete(a);
        assertEquals(OK, ret);

        Sport s = new Sport();
        s.setId(1);
        s.setName("Football");
        s.setCity("Tokyo");
        s.setAthletesCount(0);//actual 0
        ret = addSport(s);
        assertEquals(OK, ret);

        ret = confirmStanding(1, 1, 1);
        assertEquals(NOT_EXISTS, ret);

        a.setId(3);
        a.setName("Maradona");
        a.setIsActive(false);
        a.setCountry("Argentina");

        ret = addAthlete(a);
        assertEquals(OK, ret);

        ret = athleteJoinSport(1, 3);
        assertEquals(OK, ret);

        ret = athleteJoinSport(1, 1);
        assertEquals(OK, ret);

        a.setId(2);
        a.setName("Arthur");
        a.setIsActive(true);
        ret = addAthlete(a);
        assertEquals(OK, ret);

        ret = athleteJoinSport(1, 2);
        assertEquals(OK, ret);

        ret = confirmStanding(1, 1, 5);
        assertEquals(BAD_PARAMS, ret);

        ret = confirmStanding(1, 3, 3);
        assertEquals(NOT_EXISTS, ret);

        ret = confirmStanding(1, 4, 3);
        assertEquals(NOT_EXISTS, ret);

        ret = confirmStanding(2, 4, 3);
        assertEquals(NOT_EXISTS, ret);

        ret = confirmStanding(2, 2, 3);
        assertEquals(NOT_EXISTS, ret);

        ret = confirmStanding(2, 2, 5);
        assertEquals(NOT_EXISTS, ret);

        ret = confirmStanding(1, 1, 1);
        assertEquals(OK, ret);

        ret = confirmStanding(1, 2, 2);
        assertEquals(OK, ret);

        /**
         * sport 1 has neymar(1) as 1, arthur(2) as 2
         */

        ret = athleteDisqualified(2, 1);
        assertEquals(NOT_EXISTS, ret);

        ret = athleteDisqualified(1, 4);
        assertEquals(NOT_EXISTS, ret);

        ret = athleteDisqualified(2, 4);
        assertEquals(NOT_EXISTS, ret);

        ret = athleteLeftSport(1, 3);
        assertEquals(OK, ret);

        ret = athleteDisqualified(1, 3);
        assertEquals(NOT_EXISTS, ret);

        ret = athleteDisqualified(1, 1);
        assertEquals(OK, ret);

        ret = athleteDisqualified(1, 2);
        assertEquals(OK, ret);

        ret = athleteDisqualified(1, 1);
        assertEquals(OK, ret);

        ret = athleteDisqualified(1, 2);
        assertEquals(OK, ret);

        ret = confirmStanding(1, 1, 1);
        assertEquals(OK, ret);

        ret = confirmStanding(1, 2, 2);
        assertEquals(OK, ret);

        ret = athleteLeftSport(1, 1);
        assertEquals(OK, ret);

        ret = athleteLeftSport(1, 2);
        assertEquals(OK, ret);

        ret = athleteDisqualified(1, 1);
        assertEquals(NOT_EXISTS, ret);

        ret = athleteDisqualified(1, 2);
        assertEquals(NOT_EXISTS, ret);
    }

    @Test
    public void FriendsTest() {
        Athlete a = new Athlete();
        a.setId(1);
        a.setName("Neymar");
        a.setCountry("Brazil");
        a.setIsActive(true);
        ReturnValue ret = addAthlete(a);
        assertEquals(OK, ret);

        a.setId(3);
        a.setName("Maradona");
        a.setIsActive(false);
        a.setCountry("Argentina");
        ret = addAthlete(a);
        assertEquals(OK, ret);

        a.setId(2);
        a.setName("Arthur");
        a.setIsActive(true);
        ret = addAthlete(a);
        assertEquals(OK, ret);

        ret = makeFriends(1, 1);
        assertEquals(BAD_PARAMS, ret);

        ret = makeFriends(1, 4);
        assertEquals(NOT_EXISTS, ret);

        ret = makeFriends(5, 4);
        assertEquals(NOT_EXISTS, ret);

        ret = makeFriends(4, 4);
        assertEquals(BAD_PARAMS, ret);

        ret = makeFriends(4, 1);
        assertEquals(NOT_EXISTS, ret);

        ret = makeFriends(1, 2);
        assertEquals(OK, ret);

        ret = makeFriends(1, 2);
        assertEquals(ALREADY_EXISTS, ret);

        ret = makeFriends(2, 1);
        assertEquals(ALREADY_EXISTS, ret);

        ret = makeFriends(1, 3);
        assertEquals(OK, ret);

        ret = makeFriends(1, 3);
        assertEquals(ALREADY_EXISTS, ret);

        ret = makeFriends(3, 2);
        assertEquals(OK, ret);

        ret = removeFriendship(3, 2);
        assertEquals(OK, ret);

        ret = removeFriendship(3, 2);
        assertEquals(NOT_EXISTS, ret);

        ret = removeFriendship(2, 3);
        assertEquals(NOT_EXISTS, ret);

        ret = removeFriendship(3, 4);
        assertEquals(NOT_EXISTS, ret);

        ret = removeFriendship(4, 4);
        assertEquals(NOT_EXISTS, ret);

        ret = removeFriendship(4, 2);
        assertEquals(NOT_EXISTS, ret);

        a.setId(1);
        deleteAthlete(a);

        ret = makeFriends(1, 2);
        assertEquals(NOT_EXISTS, ret);

        ret = makeFriends(2, 1);
        assertEquals(NOT_EXISTS, ret);

        addAthlete(a);
        ret = makeFriends(1, 2);
        assertEquals(OK, ret);

        a.setId(2);
        deleteAthlete(a);

        ret = makeFriends(1, 2);
        assertEquals(NOT_EXISTS, ret);

        ret = makeFriends(2, 1);
        assertEquals(NOT_EXISTS, ret);

    }

    @Test
    public void changePaymentTest() {
        Athlete a = new Athlete();
        a.setId(1);
        a.setName("Neymar");
        a.setCountry("Brazil");
        a.setIsActive(true);
        ReturnValue ret = addAthlete(a);
        assertEquals(OK, ret);

        Sport s = new Sport();
        s.setId(1);
        s.setName("Football");
        s.setCity("Tokyo");
        s.setAthletesCount(0);//actual 0
        ret = addSport(s);
        assertEquals(OK, ret);

        athleteJoinSport(1, 1);

        ret = changePayment(1, 1, 100);
        assertEquals(NOT_EXISTS, ret);//since participant

        ret = changePayment(999, 1, 100);
        assertEquals(NOT_EXISTS, ret);//since athlete not exists

        a.setId(3);
        a.setName("Maradona");
        a.setCountry("Argentina");
        a.setIsActive(false);
        ret = addAthlete(a);
        assertEquals(OK, ret);

        ret = changePayment(3, 1, 200);
        assertEquals(NOT_EXISTS, ret);

        athleteJoinSport(1, 3);

        ret = changePayment(3, 1, 200);
        assertEquals(OK, ret);

        ret = changePayment(4, 1, 200);
        assertEquals(NOT_EXISTS, ret);

        ret = changePayment(4, 4, 200);
        assertEquals(NOT_EXISTS, ret);

        ret = changePayment(1, 3, 200);
        assertEquals(NOT_EXISTS, ret);

        ret = changePayment(4, 1, -1);
        assertEquals(NOT_EXISTS, ret);

        ret = changePayment(1, 1, -1);
        assertEquals(NOT_EXISTS, ret);

        ret = changePayment(3, 1, -1);
        assertEquals(BAD_PARAMS, ret);

    }

    @Test
    public void AthletePopularTest() {
        Boolean ret = isAthletePopular(1);
        assertEquals(false, ret);

        Athlete a = new Athlete();
        a.setId(1);
        a.setIsActive(true);
        a.setCountry("Israel");
        a.setName("Orel Dgani");
        addAthlete(a);

        Sport s = new Sport();
        s.setId(1);
        s.setAthletesCount(0);
        s.setCity("Tokyo");
        s.setName("Football");
        addSport(s);

        a.setId(2);
        addAthlete(a);

        ret = isAthletePopular(1);
        assertEquals(true, ret);

        makeFriends(1, 2);
        athleteJoinSport(1, 1);

        ret = isAthletePopular(1);
        assertEquals(true, ret);

        ret = isAthletePopular(2);
        assertEquals(false, ret);

        a.setId(3);
        addAthlete(a);
        s.setId(2);
        addSport(s);
        makeFriends(1, 3);

        ret = isAthletePopular(1);
        assertEquals(true, ret);

        ret = isAthletePopular(3);
        assertEquals(false, ret);

        athleteJoinSport(2, 3);
        ret = isAthletePopular(1);
        assertEquals(false, ret);

        //1 friends with 2,3
        //1 in sport 1
        //3 in sport 3

        athleteJoinSport(2, 2);
        ret = isAthletePopular(2);
        assertEquals(false, ret);

        removeFriendship(2, 1);
        ret = isAthletePopular(2);
        assertEquals(true, ret);

        makeFriends(2, 1);
        athleteJoinSport(1, 2);
        ret = isAthletePopular(2);
        assertEquals(true, ret);

        ret = isAthletePopular(1);
        assertEquals(false, ret);

        Solution.clearTables();

        for (int i = 1; i <= 15; i++) {
            a.setId(i);
            s.setId(i);
            addAthlete(a);
            addSport(s);
            assertEquals(OK, athleteJoinSport(i, i));
        }
        for (int i = 1; i <= 13; i++) {
            assertEquals(OK, makeFriends(i, i + 2));
            ret = isAthletePopular(i);
            assertEquals(false, ret);
        }
        ret = isAthletePopular(14);
        assertEquals(false, ret);
        ret = isAthletePopular(15);
        assertEquals(false, ret);

    }

    @Test
    public void CountryMedalsTest() {
        Integer num = getTotalNumberOfMedalsFromCountry("Israel");
        assertEquals(Integer.valueOf(0), num);

        Athlete a = new Athlete();
        a.setId(1);
        a.setName("Neymar");
        a.setCountry("Brazil");
        a.setIsActive(true);
        ReturnValue ret = addAthlete(a);
        assertEquals(OK, ret);

        Sport s = new Sport();
        s.setId(1);
        s.setName("Football");
        s.setCity("Tokyo");
        s.setAthletesCount(0);//actual 0
        ret = addSport(s);
        assertEquals(OK, ret);

        athleteJoinSport(1, 1);

        num = getTotalNumberOfMedalsFromCountry("Brazil");
        assertEquals(Integer.valueOf(0), num);

        confirmStanding(1, 1, 1);

        num = getTotalNumberOfMedalsFromCountry("Brazil");
        assertEquals(Integer.valueOf(1), num);

        a.setId(2);
        a.setName("Arthur");
        ret = addAthlete(a);
        assertEquals(OK, ret);

        a.setId(3);
        a.setName("Maradona");
        a.setCountry("Argentina");
        a.setIsActive(false);
        ret = addAthlete(a);
        assertEquals(OK, ret);

        athleteJoinSport(1, 3);

        num = getTotalNumberOfMedalsFromCountry("Argentina");
        assertEquals(Integer.valueOf(0), num);

        athleteJoinSport(1, 2);

        num = getTotalNumberOfMedalsFromCountry("Brazil");
        assertEquals(Integer.valueOf(1), num);

        confirmStanding(1, 2, 1);

        num = getTotalNumberOfMedalsFromCountry("Brazil");
        assertEquals(Integer.valueOf(2), num);

        athleteDisqualified(1, 2);

        num = getTotalNumberOfMedalsFromCountry("Brazil");
        assertEquals(Integer.valueOf(1), num);

        s.setId(2);
        s.setName("Basketball");
        s.setCity("Tokyo");
        s.setAthletesCount(0);//actual 0
        ret = addSport(s);
        assertEquals(OK, ret);

        for (int i = 4; i <= 20; i++) {
            a.setId(i);
            a.setIsActive(true);
            a.setCountry("Brazil");
            addAthlete(a);
            ret = athleteJoinSport(2, i);
            assertEquals(OK, ret);
            ret = confirmStanding(2, i, 2);
            assertEquals(OK, ret);
        }

        num = getTotalNumberOfMedalsFromCountry("Brazil");
        assertEquals(Integer.valueOf(18), num);

        for (int i = 21; i <= 30; i++) {
            a.setId(i);
            a.setIsActive(true);
            a.setCountry("Israel");
            addAthlete(a);
            if (i % 2 == 0) {
                athleteJoinSport(1, i);
                confirmStanding(1, i, 2);
            } else {
                athleteJoinSport(2, i);
                confirmStanding(2, i, 3);
            }
        }

        num = getTotalNumberOfMedalsFromCountry("Israel");
        assertEquals(Integer.valueOf(10), num);
    }

    @Test
    public void IncomeFromSportTest() {
        Athlete a = new Athlete();
        a.setId(1);
        a.setName("Neymar");
        a.setCountry("Brazil");
        a.setIsActive(false);
        ReturnValue ret = addAthlete(a);
        assertEquals(OK, ret);

        Sport s = new Sport();
        s.setId(1);
        s.setName("Football");
        s.setCity("Tokyo");
        s.setAthletesCount(0);//actual 0
        ret = addSport(s);
        assertEquals(OK, ret);

        Integer num = getIncomeFromSport(1);
        assertEquals(Integer.valueOf(0), num);

        num = getIncomeFromSport(2);
        assertEquals(Integer.valueOf(0), num);

        athleteJoinSport(1, 1);

        num = getIncomeFromSport(1);
        assertEquals(Integer.valueOf(100), num);

        a.setId(2);
        a.setName("Neymar");
        a.setCountry("Brazil");
        a.setIsActive(true);
        ret = addAthlete(a);
        assertEquals(OK, ret);

        athleteJoinSport(1, 2);

        num = getIncomeFromSport(1);
        assertEquals(Integer.valueOf(100), num);

        changePayment(2, 1, 30);

        num = getIncomeFromSport(1);
        assertEquals(Integer.valueOf(100), num);

        changePayment(1, 1, -20);

        num = getIncomeFromSport(1);
        assertEquals(Integer.valueOf(100), num);

        changePayment(1, 1, 75);

        num = getIncomeFromSport(1);
        assertEquals(Integer.valueOf(75), num);

        for (int i = 3; i <= 20; i++) {
            a.setIsActive(false);
            a.setId(i);
            addAthlete(a);
            ret = athleteJoinSport(1, i);
            assertEquals(OK, ret);
            ret = changePayment(i, 1, i * 10);
            assertEquals(OK, ret);
        }

        for (int i = 21; i <= 25; i++) {
            a.setId(i);
            addAthlete(a);
            ret = athleteJoinSport(1, i);
            assertEquals(OK, ret);
        }

        for (int i = 26; i <= 30; i++) {
            a.setIsActive(true);
            a.setId(i);
            addAthlete(a);
            ret = athleteJoinSport(1, i);
            assertEquals(OK, ret);
        }

        num = getIncomeFromSport(1);
        assertEquals(Integer.valueOf(2645), num);

    }

    @Test
    public void BestCountryTest() {
        String output = getBestCountry();
        assertEquals("", output);

        Athlete a = new Athlete();
        a.setId(1);
        a.setName("Neymar");
        a.setCountry("Brazil");
        a.setIsActive(true);
        ReturnValue ret = addAthlete(a);
        assertEquals(OK, ret);

        output = getBestCountry();
        assertEquals("", output);

        Sport s = new Sport();
        s.setId(1);
        s.setName("Football");
        s.setCity("Tokyo");
        s.setAthletesCount(0);//actual 0
        ret = addSport(s);
        assertEquals(OK, ret);

        athleteJoinSport(1, 1);

        output = getBestCountry();
        assertEquals("", output);
        confirmStanding(1, 1, 1);
        for (int i = 2; i <= 20; i++) {
            a.setId(i);
            if (i % 2 == 0)
                a.setCountry("Israel");
            else
                a.setCountry("Brazil");
            s.setId(i);
            addAthlete(a);
            addSport(s);
            athleteJoinSport(i, i);
            confirmStanding(i, i, 2);
        }

        output = getBestCountry();
        assertEquals("Brazil", output);

        athleteDisqualified(19, 19);
        output = getBestCountry();
        assertEquals("Israel", output);

        confirmStanding(19, 19, 1);
        a.setCountry("Argentina");
        for (int i = 21; i <= 30; i++) {
            a.setId(i);
            addAthlete(a);
            athleteJoinSport(15, i);
            confirmStanding(15, i, 3);
        }

        output = getBestCountry();
        assertEquals("Argentina", output);

        athleteDisqualified(15, 21);
        output = getBestCountry();
        assertEquals("Brazil", output);

        confirmStanding(15, 21, 1);
        athleteDisqualified(19, 19);
        output = getBestCountry();
        assertEquals("Argentina", output);

        athleteDisqualified(15, 21);
        output = getBestCountry();
        assertEquals("Israel", output);

        Solution.clearTables();

        for (int i = 1; i <= 20; i++) {
            a.setId(i);
            String aRepeated = IntStream.range(0, 10 - (i % 10)).mapToObj(j -> "a").collect(Collectors.joining(""));
            a.setCountry(aRepeated);
            addAthlete(a);
            s.setId(i);
            addSport(s);
            athleteJoinSport(i, i);
            confirmStanding(i, i, 1);
        }

        output = getBestCountry();
        assertEquals("a", output);

        for (int i = 19; i >= 11; i--) {
            athleteDisqualified(i, i);
            String aRepeated = IntStream.range(0, 10 - (i % 10) + 1).mapToObj(j -> "a").collect(Collectors.joining(""));
            output = getBestCountry();
            assertEquals(aRepeated, output);
        }
        athleteDisqualified(10, 10);
        String aRepeated = IntStream.range(0, 1).mapToObj(j -> "a").collect(Collectors.joining(""));
        output = getBestCountry();
        assertEquals(aRepeated, output);
    }

    @Test
    public void PopularCityTest() {
        String output = getMostPopularCity();
        assertEquals("", output);

        Sport s = new Sport();
        s.setId(2);
        s.setName("Tennis");
        s.setCity("Haifa");
        s.setAthletesCount(0);
        addSport(s);

        output = getMostPopularCity();
        assertEquals("Haifa", output);

        s.setId(1);
        s.setCity("Tokyo");
        addSport(s);

        output = getMostPopularCity();
        assertEquals("Tokyo", output);

        Athlete a = new Athlete();
        a.setCountry("Israel");
        a.setId(1);
        a.setIsActive(true);
        a.setName("Dudi Sela");
        addAthlete(a);

        a.setId(2);
        addAthlete(a);

        athleteJoinSport(2, 1);
        athleteJoinSport(2, 2);
        output = getMostPopularCity();
        assertEquals("Haifa", output);

        s.setId(3);
        s.setCity("Tokyo");
        addSport(s);

        athleteJoinSport(1, 1);
        athleteJoinSport(1, 2);

        output = getMostPopularCity();
        assertEquals("Haifa", output);

        s.setId(4);
        s.setCity("Haifa");
        addSport(s);

        output = getMostPopularCity();
        assertEquals("Tokyo", output);

        // 2 athletes in 2 sports

        for (int i = 5; i <= 10; i++) {
            s.setId(i);
            String aRepeated = IntStream.range(0, i - 4).mapToObj(j -> "z").collect(Collectors.joining(""));
            s.setCity(aRepeated);
            addSport(s);
            athleteJoinSport(i, 1);
        }

        output = getMostPopularCity();
        assertEquals("zzzzzz", output);

        athleteLeftSport(10, 1);

        output = getMostPopularCity();
        assertEquals("zzzzz", output);

        athleteLeftSport(9, 1);

        output = getMostPopularCity();
        assertEquals("zzzz", output);

        athleteLeftSport(8, 1);

        output = getMostPopularCity();
        assertEquals("zzz", output);

        athleteLeftSport(7, 1);

        output = getMostPopularCity();
        assertEquals("zz", output);

        athleteLeftSport(6, 1);

        output = getMostPopularCity();
        assertEquals("z", output);

        athleteLeftSport(5, 1);

        output = getMostPopularCity();
        assertEquals("Tokyo", output);

        athleteLeftSport(1, 1);

        output = getMostPopularCity();
        assertEquals("Haifa", output);

    }

    /**
     * ADVANCED API
     */
    @Test
    public void AthletesMedalsTest() {
        Athlete a = new Athlete();
        a.setId(1);
        a.setIsActive(true);
        a.setCountry("Israel");
        a.setName("Orel Dgani");
        addAthlete(a);

        ArrayList<Integer> medals = getAthleteMedals(1);
        assertEquals((Integer) 0, medals.get(0));
        assertEquals((Integer) 0, medals.get(1));
        assertEquals((Integer) 0, medals.get(2));

        medals = getAthleteMedals(2);
        assertEquals((Integer) 0, medals.get(0));
        assertEquals((Integer) 0, medals.get(1));
        assertEquals((Integer) 0, medals.get(2));


        Sport s = new Sport();
        s.setId(1);
        s.setAthletesCount(0);
        s.setCity("Tokyo");
        s.setName("Football");
        addSport(s);

        athleteJoinSport(1, 1);

        medals = getAthleteMedals(1);
        assertEquals((Integer) 0, medals.get(0));
        assertEquals((Integer) 0, medals.get(1));
        assertEquals((Integer) 0, medals.get(2));

        confirmStanding(1, 1, 1);

        medals = getAthleteMedals(1);
        assertEquals((Integer) 1, medals.get(0));
        assertEquals((Integer) 0, medals.get(1));
        assertEquals((Integer) 0, medals.get(2));

        s.setId(2);
        addSport(s);
        athleteJoinSport(2, 1);
        confirmStanding(2, 1, 2);

        medals = getAthleteMedals(1);
        assertEquals((Integer) 1, medals.get(0));
        assertEquals((Integer) 1, medals.get(1));
        assertEquals((Integer) 0, medals.get(2));

        s.setId(3);
        addSport(s);
        athleteJoinSport(3, 1);
        confirmStanding(3, 1, 3);

        medals = getAthleteMedals(1);
        assertEquals((Integer) 1, medals.get(0));
        assertEquals((Integer) 1, medals.get(1));
        assertEquals((Integer) 1, medals.get(2));

        s.setId(4);
        addSport(s);
        athleteJoinSport(4, 1);
        confirmStanding(4, 1, 3);

        medals = getAthleteMedals(1);
        assertEquals((Integer) 1, medals.get(0));
        assertEquals((Integer) 1, medals.get(1));
        assertEquals((Integer) 2, medals.get(2));

        for (int i = 6; i <= 14; i++) {
            s.setId(i);
            addSport(s);
            athleteJoinSport(i, 1);
            confirmStanding(i, 1, i % 3 + 1);

            medals = getAthleteMedals(1);
            assertEquals((Integer) (i / 3), medals.get(0));
            assertEquals((Integer) ((i - 1) / 3), medals.get(1));
            assertEquals((Integer) ((i - 2) / 3 + 1), medals.get(2));
        }

        medals = getAthleteMedals(999);
        assertEquals((Integer) 0, medals.get(0));
        assertEquals((Integer) 0, medals.get(1));
        assertEquals((Integer) 0, medals.get(2));
    }

    @Test
    public void MostRatedAthletes() {
        ArrayList<Integer> pop = getMostRatedAthletes();
        assertEquals(0, pop.size());

        Athlete a = new Athlete();
        a.setId(1);
        a.setIsActive(true);
        a.setCountry("Israel");
        a.setName("Orel Dgani");
        addAthlete(a);

        //athlete 1

        pop = getMostRatedAthletes();
        assertEquals(1, pop.size());
        assertEquals((Integer) 1, pop.get(0));


        a.setId(2);
        addAthlete(a);

        //athlete 1,2

        pop = getMostRatedAthletes();
        assertEquals((Integer) 1, pop.get(0));
        assertEquals((Integer) 2, pop.get(1));
        assertEquals(2, pop.size());

        Sport s = new Sport();
        s.setId(1);
        s.setAthletesCount(0);
        s.setCity("Tokyo");
        s.setName("Football");
        addSport(s);

        athleteJoinSport(1, 1);

        pop = getMostRatedAthletes();
        assertEquals((Integer) 1, pop.get(0));
        assertEquals((Integer) 2, pop.get(1));
        assertEquals(2, pop.size());

        athleteJoinSport(1, 2);
        confirmStanding(1, 2, 1);

        pop = getMostRatedAthletes();
        assertEquals((Integer) 2, pop.get(0));
        assertEquals((Integer) 1, pop.get(1));
        assertEquals(2, pop.size());

        //athlete 1 - 0
        //athlete 2 - 3

        for (int i = 3; i <= 15; i++) {
            a.setId(i);
            addAthlete(a);
            if (i % 2 == 0)
                athleteJoinSport(1, i);
            if (i % 4 == 0)
                confirmStanding(1, i, 2);
        }
        pop = getMostRatedAthletes();
        assertEquals((Integer) 2, pop.get(0));
        assertEquals((Integer) 4, pop.get(1));
        assertEquals((Integer) 8, pop.get(2));
        assertEquals((Integer) 12, pop.get(3));
        assertEquals((Integer) 1, pop.get(4));
        assertEquals((Integer) 3, pop.get(5));
        assertEquals((Integer) 5, pop.get(6));
        assertEquals((Integer) 6, pop.get(7));
        assertEquals((Integer) 7, pop.get(8));
        assertEquals((Integer) 9, pop.get(9));
        assertEquals(10, pop.size());

        confirmStanding(1, 14, 1);

        pop = getMostRatedAthletes();
        assertEquals((Integer) 2, pop.get(0));//3
        assertEquals((Integer) 14, pop.get(1));//3
        assertEquals((Integer) 4, pop.get(2));//2
        assertEquals((Integer) 8, pop.get(3));//2
        assertEquals((Integer) 12, pop.get(4));//2
        assertEquals((Integer) 1, pop.get(5));//others 0
        assertEquals((Integer) 3, pop.get(6));
        assertEquals((Integer) 5, pop.get(7));
        assertEquals((Integer) 6, pop.get(8));
        assertEquals((Integer) 7, pop.get(9));
        assertEquals(10, pop.size());

        s.setId(2);
        addSport(s);

        athleteJoinSport(2, 12);
        confirmStanding(2, 12, 3);

        pop = getMostRatedAthletes();
        assertEquals((Integer) 2, pop.get(0));//3
        assertEquals((Integer) 12, pop.get(1));//3
        assertEquals((Integer) 14, pop.get(2));//3
        assertEquals((Integer) 4, pop.get(3));//2
        assertEquals((Integer) 8, pop.get(4));//2
        assertEquals((Integer) 1, pop.get(5));//others 0
        assertEquals((Integer) 3, pop.get(6));
        assertEquals((Integer) 5, pop.get(7));
        assertEquals((Integer) 6, pop.get(8));
        assertEquals((Integer) 7, pop.get(9));
        assertEquals(10, pop.size());

        confirmStanding(2, 12, 2);

        pop = getMostRatedAthletes();
        assertEquals((Integer) 12, pop.get(0));//4
        assertEquals((Integer) 2, pop.get(1));//3
        assertEquals((Integer) 14, pop.get(2));//3
        assertEquals((Integer) 4, pop.get(3));//2
        assertEquals((Integer) 8, pop.get(4));//2
        assertEquals((Integer) 1, pop.get(5));//others 0
        assertEquals((Integer) 3, pop.get(6));
        assertEquals((Integer) 5, pop.get(7));
        assertEquals((Integer) 6, pop.get(8));
        assertEquals((Integer) 7, pop.get(9));
        assertEquals(10, pop.size());

        athleteJoinSport(1, 15);
        athleteJoinSport(2, 15);
        confirmStanding(1, 15, 1);
        confirmStanding(2, 15, 1);

        pop = getMostRatedAthletes();
        assertEquals((Integer) 15, pop.get(0));//6
        assertEquals((Integer) 12, pop.get(1));//4
        assertEquals((Integer) 2, pop.get(2));//3
        assertEquals((Integer) 14, pop.get(3));//3
        assertEquals((Integer) 4, pop.get(4));//2
        assertEquals((Integer) 8, pop.get(5));//2
        assertEquals((Integer) 1, pop.get(6));//others 0
        assertEquals((Integer) 3, pop.get(7));
        assertEquals((Integer) 5, pop.get(8));
        assertEquals((Integer) 6, pop.get(9));
        assertEquals(10, pop.size());

        for (int i = 1; i <= 15; i++) {
            athleteJoinSport(1, i);
            athleteJoinSport(2, i);
            if (i % 5 == 0)
                confirmStanding(1, i, 2);
        }

        pop = getMostRatedAthletes();
        assertEquals((Integer) 15, pop.get(0));//5
        assertEquals((Integer) 12, pop.get(1));//4
        assertEquals((Integer) 2, pop.get(2));//3
        assertEquals((Integer) 14, pop.get(3));//3
        assertEquals((Integer) 4, pop.get(4));//2
        assertEquals((Integer) 5, pop.get(5));//2
        assertEquals((Integer) 8, pop.get(6));//2
        assertEquals((Integer) 10, pop.get(7));//2
        assertEquals((Integer) 1, pop.get(8));//others 0
        assertEquals((Integer) 3, pop.get(9));
        assertEquals(10, pop.size());

    }

    @Test
    public void CloseAthletesTest() {
        ArrayList<Integer> pop = getCloseAthletes(1);
        assertEquals(0, pop.size());

        Athlete a = new Athlete();
        a.setId(1);
        a.setIsActive(true);
        a.setCountry("Israel");
        a.setName("Orel Dgani");
        addAthlete(a);

        Sport s = new Sport();
        s.setId(1);
        s.setAthletesCount(0);
        s.setCity("Tokyo");
        s.setName("Football");
        addSport(s);

        pop = getCloseAthletes(1);
        assertEquals(0, pop.size());

        a.setId(2);
        a.setIsActive(false);
        addAthlete(a);

        s.setId(2);
        addSport(s);

        pop = getCloseAthletes(1);
        assertEquals((Integer) 2, pop.get(0));
        assertEquals(1, pop.size());

        athleteJoinSport(1, 2);

        pop = getCloseAthletes(1);
        assertEquals((Integer) 2, pop.get(0));
        assertEquals(1, pop.size());

        athleteJoinSport(2, 2);

        pop = getCloseAthletes(2);
        assertEquals(0, pop.size());

        athleteJoinSport(1, 1);

        pop = getCloseAthletes(2);
        assertEquals((Integer) 1, pop.get(0));
        assertEquals(1, pop.size());

        pop = getCloseAthletes(1);
        assertEquals((Integer) 2, pop.get(0));
        assertEquals(1, pop.size());

        athleteLeftSport(2, 2);
        //both in sport 1

        for (int i = 3; i <= 15; i++) {
            a.setId(i);
            a.setIsActive(true);
            addAthlete(a);
            athleteJoinSport(1, i);
        }

        //all in sport 1
        for (int i = 1; i <= 15; i++) {
            pop = getCloseAthletes(i);
            int k = 0;
            for (int j = 1; j <= 11; j++) {
                if (j != i && k <= 9) {
                    assertEquals((Integer) j, pop.get(k));
                    k++;
                }
            }
            assertEquals(10, pop.size());
        }

        athleteLeftSport(1, 3);
        athleteLeftSport(1, 7);
        athleteLeftSport(1, 10);

        pop = getCloseAthletes(1);
        assertEquals((Integer) 2, pop.get(0));
        assertEquals((Integer) 4, pop.get(1));
        assertEquals((Integer) 5, pop.get(2));
        assertEquals((Integer) 6, pop.get(3));
        assertEquals((Integer) 8, pop.get(4));
        assertEquals((Integer) 9, pop.get(5));
        assertEquals((Integer) 11, pop.get(6));
        assertEquals((Integer) 12, pop.get(7));
        assertEquals((Integer) 13, pop.get(8));
        assertEquals((Integer) 14, pop.get(9));
        assertEquals(10, pop.size());

        pop = getCloseAthletes(999);
        assertEquals(0,pop.size());

    }

    @Test
    public void RecommendationTest() {
        ArrayList<Integer> pop = getSportsRecommendation(1);
        assertEquals(0, pop.size());

        Athlete a = new Athlete();
        a.setId(1);
        a.setIsActive(true);
        a.setCountry("Israel");
        a.setName("Orel Dgani");
        addAthlete(a);

        Sport s = new Sport();
        s.setId(1);
        s.setAthletesCount(0);
        s.setCity("Tokyo");
        s.setName("Football");
        addSport(s);

        pop = getCloseAthletes(1);
        assertEquals(0, pop.size());

        a.setId(2);
        a.setIsActive(false);
        addAthlete(a);

        s.setId(2);
        addSport(s);
        athleteJoinSport(1, 2);

        pop = getSportsRecommendation(1);
        assertEquals((Integer) 1, pop.get(0));
        assertEquals(1, pop.size());

        athleteJoinSport(1, 1);

        pop = getSportsRecommendation(1);
        assertEquals(0, pop.size());

        s.setId(2);
        addSport(s);

        s.setId(3);
        addSport(s);

        s.setId(4);
        addSport(s);

        athleteLeftSport(1, 1);
        athleteJoinSport(2, 2);
        athleteJoinSport(3, 2);
        //sport1,2,3 has 2

        pop = getSportsRecommendation(1);
        assertEquals((Integer) 1, pop.get(0));
        assertEquals((Integer) 2, pop.get(1));
        assertEquals((Integer) 3, pop.get(2));
        assertEquals(3, pop.size());

        a.setId(3);
        addAthlete(a);
        athleteJoinSport(1, 3);

        //sport1 has 2,3 so athlete1 has 2,3 as close
        athleteJoinSport(4, 2);
        athleteJoinSport(2, 3);
        athleteJoinSport(3, 3);
        athleteJoinSport(4, 3);

        pop = getSportsRecommendation(1);
        assertEquals((Integer) 1, pop.get(0));
        assertEquals((Integer) 2, pop.get(1));
        assertEquals((Integer) 3, pop.get(2));
        assertEquals(3, pop.size());

        //each sport1,2,3,4 has 2 athletes

        athleteLeftSport(2, 2);
        pop = getSportsRecommendation(1);
        assertEquals((Integer) 1, pop.get(0));
        assertEquals((Integer) 3, pop.get(1));
        assertEquals((Integer) 4, pop.get(2));
        assertEquals(3, pop.size());

        //each sport1,3,4 has 2 athletes
        //sport 2 has 1 athlete

        pop = getSportsRecommendation(3);
        assertEquals(0, pop.size());

        Solution.clearTables();

        a.setId(1);
        addAthlete(a);
        a.setId(2);
        addAthlete(a);
        s.setId(1);
        addSport(s);
        s.setId(2);
        addSport(s);
        s.setId(3);
        addSport(s);

        athleteJoinSport(1, 1);
        athleteJoinSport(1, 2);
        athleteJoinSport(2, 1);
        athleteJoinSport(3, 2);

        pop = getSportsRecommendation(1);
        assertEquals((Integer) 3, pop.get(0));
        assertEquals(1, pop.size());

        athleteLeftSport(1, 2);
        pop = getSportsRecommendation(1);
        assertEquals(0, pop.size());

        athleteJoinSport(1, 2);
        athleteLeftSport(3, 2);
        pop = getSportsRecommendation(1);
        assertEquals(0, pop.size());

        pop = getSportsRecommendation(999);
        assertEquals(0, pop.size());

    }
}