package pl.frati.dynx.tasks;

import org.testng.Assert;
import org.testng.annotations.Test;

import pl.frati.dynx.tasks.Task.State;

public class AbstractThreadTaskTest {

	@Test
	public void checkExecution() throws InterruptedException {
		
		TestTask task = new TestTask(4);
		
		System.out.println(task);
		
		Assert.assertEquals(task.getCurrentState(), State.NOT_STARTED);
		
		task.requestStart();

		System.out.println(task);
		Thread.sleep(100);
		System.out.println(task);

		
		Assert.assertEquals(task.getCurrentState(), State.RUNNING);
		
		Thread.sleep(100);
		System.out.println(task);
		
		Assert.assertEquals(task.getCurrentState(), State.RUNNING);
		
		Thread.sleep(400);
		System.out.println(task);
		
		Assert.assertEquals(task.getCurrentState(), State.FINISHED);
	}
	
	@Test
	public void checkPause() throws InterruptedException {
		
		TestTask task = new TestTask(5);
		
		System.out.println(task);
		
		Assert.assertEquals(task.getCurrentState(), State.NOT_STARTED);
		
		task.requestStart();

		System.out.println(task);
		Thread.sleep(100);
		System.out.println(task);

		
		Assert.assertEquals(task.getCurrentState(), State.RUNNING);
		
		task.requestPause();
		Thread.sleep(100);
		System.out.println(task);
		
		Assert.assertEquals(task.getCurrentState(), State.PAUSED);
		
		Thread.sleep(400);
		System.out.println(task);
		
		Assert.assertEquals(task.getCurrentState(), State.PAUSED);
		
		task.requestResume();
		
		Thread.sleep(100);
		System.out.println(task);
		
		Assert.assertEquals(task.getCurrentState(), State.RUNNING);
		
		Thread.sleep(400);
		System.out.println(task);
		
		Assert.assertEquals(task.getCurrentState(), State.FINISHED);
	}
	
	@Test
	public void checkStop() throws InterruptedException {
		
		TestTask task = new TestTask(4);
		
		System.out.println(task);
		
		Assert.assertEquals(task.getCurrentState(), State.NOT_STARTED);
		
		task.requestStart();

		System.out.println(task);
		Thread.sleep(100);
		System.out.println(task);

		
		Assert.assertEquals(task.getCurrentState(), State.RUNNING);
		
		task.requestStop();
		Thread.sleep(100);
		System.out.println(task);
		
		Assert.assertEquals(task.getCurrentState(), State.STOPPPED);
		
		Thread.sleep(400);
		System.out.println(task);
		
		Assert.assertEquals(task.getCurrentState(), State.STOPPPED);
	}
	
	private static final class TestTask extends AbstractThreadTask {

		private int loops;
		private int currentLoop;

		public TestTask(int loops) {
			super();
			this.loops = loops;
		}

		@Override
		protected boolean hasNextPortion() {
			return currentLoop < loops;
		}

		@Override
		protected void executeNextPortion() {
			try {
				Thread.sleep(100);
				currentLoop++;
			} catch (InterruptedException e) {
				throw new RuntimeException("Interrupted", e);
			}
		}

	}
}