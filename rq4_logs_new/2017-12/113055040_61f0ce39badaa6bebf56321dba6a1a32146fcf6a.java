package bgu.spl.a2;

import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class PromiseTest {
    private Promise<Integer> p;

    /**
     * Setup for a test
     */

    @Before
    public void setUp() throws Exception {
        p = new Promise<Integer>();
    }

    /**
     * tears down the test subject
     * @throws Exception
     */
    @After
    public void tearDown() throws Exception {
        p = null;
    }
    @Test
    public void TestGet() {
        try {
            Integer toGet = p.get();
            assertFalse(toGet == null);
        } catch (IllegalStateException e) {
            Assert.fail();
        } catch (Exception e) {
            Assert.fail();
        }
    }

    @Test
    public void TestResolve() {
        try{
            p.resolve(5);
            try{
                p.resolve(6);
                Assert.fail();
            }catch(IllegalStateException e){
                int x = p.get();
                assertEquals( x, 5);
            }catch(Exception e){
                Assert.fail();
            }
        }catch(Exception e){
            Assert.fail();
        }
    }

    @Test
    public void TestIsResolved() {
        try{
            assertTrue(!p.isResolved());
            try{
                p.resolve(6);
                assertTrue(p.isResolved());
            }catch(IllegalStateException e){
                Assert.fail();
            }catch(Exception e){
                Assert.fail();
            }
        }catch(Exception e){
            Assert.fail();
        }
    }

    @Test
    public void testSubscribe(callback cb){
        if (!p.isResolved())
            assertTrue(cb == null);
        else{
            assertEquals(cb, p.get());
        }
    }
}