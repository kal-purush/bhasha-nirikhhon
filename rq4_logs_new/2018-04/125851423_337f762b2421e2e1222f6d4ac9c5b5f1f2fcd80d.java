package Model;

import junit.framework.TestCase;
import org.junit.Test;

import java.util.Date;

public class TasksListTest extends TestCase {

    private  TasksList tasksList=new TasksList();

    public void setUp() throws Exception {
        super.setUp();
    }

    public void tearDown() throws Exception {
    }



    @Test
    public void testAddToArrayList() {

        TaskDTO task = new TaskDTO("read" ,new Date(),false ,"project");
        tasksList.addToArrayList(task);
        assertEquals(task, tasksList.getArrayList().get(0));

    }
}