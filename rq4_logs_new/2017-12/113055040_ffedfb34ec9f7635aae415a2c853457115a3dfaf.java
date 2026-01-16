import bgu.spl.a2.VersionMonitor;
import junit.framework.Assert;
import junit.framework.TestCase;

public class TestVersionMonitor extends TestCase {
	/**Class Under a Test*/
	private VersionMonitor vm;
	/**
	 *
     * Set up for a test.
     */
    @Before protected void setUp() throws Exception {
        vm = CreateVersionMonitor();
    }
    
    /**
     * Creates the Object instance
     * @return a {@VersionMonitor} instance
     */
    protected VersionMonitor CreateVersionMonitor(){
    	return new VersionMonitor();
    }
	
	/**
	 * Tests The Method getVersion
	 * @Pre: none
	 * @Post: getVersion = x+1
	 * */
	@Test
	public void TestGetVersion(){
		try{
			int x = vm.getVersion();
			try{
				vm.inc();
				int y = vm.getVersion();
				assertEquals(x+1, y);
			}
			catch(Exception e){
				Assert.fail();				
			}
		}catch(Exception e){
			Assert.fail();
		}
	}
	
	
	/**
	 * Tests The Method Inc()
	 * @Pre: none
	 * @Post: getVersion = x+1
	 * 
	 * */
	@Test public void TestInc(){
		try{
			int x = vm.getVersion();
			try{
				vm.inc();
				int y = vm.getVersion();
				assertEquals(x+1, y);
			}
			catch(Exception e){
				Assert.fail();				
			}
		}catch(Exception e){
			Assert.fail();
		}
	}
	
	/**
	 * Tests The Method Await()
	 * @Pre: none
	 * @Post: getVersion = x+1
	 * 
	 * */
	@Test public void TestAwait(){
		try{
			int x = vm.getVersion();
			try{
				vm.await(x);
				vm.inc();
				int y = vm.getVersion();
				assertEquals(x+1, y);
			}
			catch(Exception e){
				Assert.fail();				
			}
		}catch(Exception e){
			Assert.fail();
		}
	}
}
