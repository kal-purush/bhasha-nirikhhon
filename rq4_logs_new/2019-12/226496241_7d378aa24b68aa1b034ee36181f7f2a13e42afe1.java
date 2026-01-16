package olympic;

import olympic.business.Athlete;
import olympic.business.Sport;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class AdvancedAPITest extends AbstractTest {

    private void addSport(int id,String name,String city){
        Sport s1 = new Sport();
        s1.setAthletesCount(0);
        s1.setCity(city);
        s1.setId(id);
        s1.setName(name);
        Solution.addSport(s1);
    }

    private void addAthlete(int id,String name,String country,boolean isActive){
        Athlete a1 = new Athlete();
        a1.setId(id);
        a1.setName(name);
        a1.setCountry(country);
        a1.setIsActive(isActive);
        Solution.addAthlete(a1);
    }

    @Test
    public void athleteMedalsTest0(){

        List<Integer> res = Solution.getAthleteMedals(11);
        int a = res.get(0);
        assertEquals(0,a);
        a = res.get(1);
        assertEquals(0,a);
        a = res.get(2);
        assertEquals(0,a);

        addSport(1,"s1","c1");
        addAthlete(11,"a11","c11",true);
        Solution.athleteJoinSport(1,11);
        res = Solution.getAthleteMedals(11);
        a = res.get(0);
        assertEquals(0,a);
        a = res.get(1);
        assertEquals(0,a);
        a = res.get(2);
        assertEquals(0,a);

        Solution.confirmStanding(1,11,1);
        res = Solution.getAthleteMedals(11);
        a = res.get(0);
        assertEquals(1,a);
        a = res.get(1);
        assertEquals(0,a);
        a = res.get(2);
        assertEquals(0,a);
    }

    @Test
    public void athleteMedalsTest1(){
        addSport(1,"s1","c1");
        addAthlete(11,"a11","c11",true);
        Solution.athleteJoinSport(1,11);
        Solution.confirmStanding(1,11,1);
        List<Integer> res = Solution.getAthleteMedals(11);
        int a = res.get(0);
        assertEquals(1,a);
        a = res.get(1);
        assertEquals(0,a);
        a = res.get(2);
        assertEquals(0,a);
    }

    @Test
    public void athleteMedalsTest2(){
        addSport(1,"s1","c1");
        addSport(2,"s2","c2");
        addSport(3,"s3","c3");
        addAthlete(11,"a11","c11",true);
        Solution.athleteJoinSport(1,11);
        Solution.confirmStanding(1,11,1);
        Solution.athleteJoinSport(2,11);
        Solution.confirmStanding(2,11,1);
        Solution.athleteJoinSport(3,11);
        Solution.confirmStanding(3,11,1);
        List<Integer> res = Solution.getAthleteMedals(11);
        int a = res.get(0);
        assertEquals(3,a);
        a = res.get(1);
        assertEquals(0,a);
        a = res.get(2);
        assertEquals(0,a);
    }

    @Test
    public void athleteMedalsTest3(){
        addSport(1,"s1","c1");
        addSport(2,"s2","c2");
        addSport(3,"s3","c3");
        addAthlete(11,"a11","c11",true);
        Solution.athleteJoinSport(1,11);
        Solution.confirmStanding(1,11,1);
        Solution.athleteJoinSport(2,11);
        Solution.confirmStanding(2,11,2);
        Solution.athleteJoinSport(3,11);
        Solution.confirmStanding(3,11,2);
        List<Integer> res = Solution.getAthleteMedals(11);
        int a = res.get(0);
        assertEquals(1,a);
        a = res.get(1);
        assertEquals(2,a);
        a = res.get(2);
        assertEquals(0,a);

        Solution.confirmStanding(3,11,3);
        res = Solution.getAthleteMedals(11);
        a = res.get(0);
        assertEquals(1,a);
        a = res.get(1);
        assertEquals(1,a);
        a = res.get(2);
        assertEquals(1,a);
    }

    @Test
    public void mostRatedTest0(){
       List<Integer> l = Solution.getMostRatedAthletes();
       assertEquals(0,l.size());

    }

    @Test
    public void mostRatedTest1(){
        addSport(1,"s1","c1");
        addSport(2,"s2","c2");
        addSport(3,"s3","c3");
        addAthlete(11,"a11","c11",true);
        addAthlete(12,"a11","c11",true);
        addAthlete(13,"a11","c11",true);
        addAthlete(14,"a11","c11",true);
        addAthlete(15,"a11","c11",false);
        Solution.athleteJoinSport(1,11);
        Solution.confirmStanding(1,11,1);
        Solution.athleteJoinSport(2,11);
        Solution.confirmStanding(2,11,2);
        Solution.athleteJoinSport(3,11);
        Solution.confirmStanding(3,11,2);
        Solution.athleteJoinSport(1,12);
        Solution.confirmStanding(1,12,1);
        Solution.athleteJoinSport(2,12);
        Solution.confirmStanding(2,12,2);
        Solution.athleteJoinSport(3,12);
        Solution.confirmStanding(3,12,1);
        Solution.athleteJoinSport(1,13);
        Solution.confirmStanding(1,13,3);
        Solution.athleteJoinSport(2,13);
        Solution.confirmStanding(2,13,2);
        Solution.athleteJoinSport(3,13);
        Solution.confirmStanding(3,13,3);
        Solution.athleteJoinSport(3,14);
        Solution.athleteJoinSport(3,15);
        List<Integer> l = Solution.getMostRatedAthletes();
        assertEquals(4,l.size());
        int a =l.get(0);
        assertEquals(12,a);
        a=l.get(1);
        assertEquals(11,a);
        a=l.get(2);
        assertEquals(13,a);
        a=l.get(3);
        assertEquals(14,a);
    }

    @Test
    public void mostRatedTest2(){
        addSport(1,"s1","c1");
        addAthlete(11,"a11","c11",true);
        addAthlete(12,"a11","c11",true);
        addAthlete(13,"a11","c11",true);
        addAthlete(14,"a11","c11",true);
        addAthlete(15,"a11","c11",true);
        addAthlete(16,"a11","c11",true);
        addAthlete(17,"a11","c11",true);
        addAthlete(18,"a11","c11",true);
        addAthlete(19,"a11","c11",true);
        addAthlete(20,"a11","c11",true);
        addAthlete(21,"a11","c11",true);
        for(int a =11;a<21;a++){
            Solution.athleteJoinSport(1,a);
            Solution.confirmStanding(1,a,1);
        }
        Solution.athleteJoinSport(1,21);
        Solution.confirmStanding(1,21,2);
        List<Integer> lst = Solution.getMostRatedAthletes();
        for(int a=11;a<21;a++){
            assertTrue(lst.contains(a));
        }
        assertFalse(lst.contains(21));
    }


}