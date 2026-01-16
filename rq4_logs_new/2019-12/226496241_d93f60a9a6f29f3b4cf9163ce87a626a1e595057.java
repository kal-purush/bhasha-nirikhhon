package olympic;

import olympic.business.Athlete;
import olympic.business.ReturnValue;
import org.junit.Test;

import static olympic.business.ReturnValue.*;
import static org.junit.Assert.assertEquals;

public class BasicAthleteTest extends AbstractTest{

    @Test
    public void simpleTestBadParamAthlete()
    {
        Athlete a = new Athlete();
        a.setId(-2);
        a.setName("Artur");
        a.setCountry("Brazil");
        a.setIsActive(true);
        ReturnValue ret = Solution.addAthlete(a);
        assertEquals(BAD_PARAMS, ret);
    }

    @Test
    public void simpleTestBadParam2Athlete()
    {
        Athlete a = new Athlete();
        a.setId(2);
        a.setName(null);
        a.setCountry("Brazil");
        a.setIsActive(true);
        ReturnValue ret = Solution.addAthlete(a);
        assertEquals(BAD_PARAMS, ret);
    }

    @Test
    public void simpleTestBadParam3Athlete()
    {
        Athlete a = new Athlete();
        a.setId(2);
        a.setName("sfdg");
        a.setCountry(null);
        a.setIsActive(true);
        ReturnValue ret = Solution.addAthlete(a);
        assertEquals(BAD_PARAMS, ret);
    }

    @Test
    public void simpleTestAlreadyExistAthlete()
    {
        Athlete a = new Athlete();
        a.setId(22);
        a.setName("Artur");
        a.setCountry("Brazil");
        a.setIsActive(true);
        ReturnValue ret = Solution.addAthlete(a);
        assertEquals(OK, ret);
        Athlete b = new Athlete();
        b.setId(22);
        b.setName("A");
        b.setCountry("B");
        b.setIsActive(true);
        ret = Solution.addAthlete(a);
        assertEquals(ALREADY_EXISTS, ret);
    }

    @Test
    public void simpleTestCreateAthlete()
    {
        Athlete a = new Athlete();
        a.setId(1);
        a.setName("Artur");
        a.setCountry("Brazil");
        a.setIsActive(true);
        ReturnValue ret = Solution.addAthlete(a);
        assertEquals(OK, ret);
    }

    @Test
    public void testDeleteUser(){
        Athlete a = new Athlete();
        a.setId(10);
        a.setName("Eli");
        a.setCountry("Argentina");
        a.setIsActive(true);
        ReturnValue ret = Solution.deleteAthlete(a);
        assertEquals(NOT_EXISTS , ret);
    }

    @Test
    public void simpleTestgetProfile()
    {
        Athlete a = new Athlete();
        a.setId(1);
        a.setName("Artur");
        a.setCountry("Brazil");
        a.setIsActive(true);
        ReturnValue ret = Solution.addAthlete(a);
        assertEquals(OK, ret);
        Athlete b = Solution.getAthleteProfile(a.getId());
        assertEquals(a,b);
    }

    @Test
    public void simpleTestgetProfile2()
    {
        Athlete a = new Athlete();
        a.setId(1);
        a.setName("Artur");
        a.setCountry("Brazil");
        a.setIsActive(true);
        Athlete b = Solution.getAthleteProfile(a.getId());
        assertEquals(b, Athlete.badAthlete());
    }

}