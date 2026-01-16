import org.junit.*;
import static org.junit.Assert.*;
import main.*;
public class LocationTest {
	static Location loc;
	@Before
	public void setup(){
		loc = new Location(0,0);	
		
	}

	@Test
	public void updateLocation() {
		loc.setLocation(1,1);
		assertEquals(new Location(1,1),loc);
	}
	@Test
	public void checkEquals() {
		assertTrue(loc.equals(loc));

	}
	@Test
	public void getParams() {
		assertEquals(0,loc.getX());
		assertEquals(0,loc.getY());
	}

	@Test
	public void initializations() {
		assertEquals(loc,new Location());
		assertEquals(new Location(), new Location(0,0));
	}

	@Test
	public void aroundTest(){
		Location a = new Location();
		assertEquals(loc.around(),a.around());
	}

}