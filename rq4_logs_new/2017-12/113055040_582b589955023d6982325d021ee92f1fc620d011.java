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
            try {
                p.resolve(6);
                assertEquals(p.get(), (Integer) 6); // todo what resolve do?
            }catch(Exception e){
                Assert.fail();
            }
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
            assertFalse(p.isResolved());
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

    public class TestCallback implements callback{
        private boolean state = false;

        public TestCallback(){

        }

        public boolean getState(){
            return state;
        }
        @Override
        public void call() {
            state = true;
        }
    }

    @Test
    public void testSubscribe(){
        try {
            TestCallback[] cb = new TestCallback[10];
            for (TestCallback callBack : cb) {
                p.subscribe(callBack);
                assertFalse(callBack.getState());
            }
            p.resolve(6);
            for (TestCallback callBack : cb) {
                assertTrue(callBack.getState());
            }

            for (TestCallback callBack : cb) {
                callBack = new TestCallback();
                p.subscribe(callBack);
                assertTrue(callBack.getState());
            }
        }catch(Exception ex){
            Assert.fail();
        }


    }
}