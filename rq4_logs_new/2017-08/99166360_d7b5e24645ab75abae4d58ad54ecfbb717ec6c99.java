package pl.frati.dynx.tasks;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class AbstractThreadTask extends Thread implements ThreadTask {

	private ReentrantReadWriteLock currentStateLock = new ReentrantReadWriteLock();
	private Task.State currentState = Task.State.NOT_STARTED;
	private List<StateObserver> stateObservers = new ArrayList<>(1);

	public AbstractThreadTask() {
		super(UUID.randomUUID().toString());
	}
	
	public AbstractThreadTask(String name) {
		super(name);
	}
	
	private void notifyObservers(Task.State oldState, Task.State newState) {
		stateObservers.forEach(so -> so.stateChanged(this, oldState, newState));
	}

	protected abstract boolean hasNextPortion();
	
	protected abstract void executeNextPortion();
	
	@Override
	public void run() {
		boolean stateChanged = false;
		Task.State oldState = null;
		try {
			currentStateLock.writeLock().lock();
			try {
				oldState = currentState;
				currentState = Task.State.RUNNING;
				stateChanged = true;
			} finally {
				currentStateLock.writeLock().unlock();
			}

			notifyObservers(oldState, currentState);

			stateChanged = false;

			while (hasNextPortion()) {
				
				executeNextPortion();

				oldState = null;
				stateChanged = false;

				currentStateLock.writeLock().lock();
				try {
					if (Task.State.PAUSING.equals(currentState)) {
						oldState = currentState;
						currentState = Task.State.PAUSED;
						stateChanged = true;
					} else if (Task.State.STOPPING.equals(currentState)) {
						oldState = currentState;
						currentState = Task.State.STOPPPED;
						stateChanged = true;
					}
				} finally {
					currentStateLock.writeLock().unlock();
				}

				if (stateChanged) {
					notifyObservers(oldState, currentState);
				}
				
				if (Task.State.PAUSED.equals(getCurrentState())) {
					synchronized (this) {
						wait();
					}
				}

				if (Task.State.STOPPPED.equals(getCurrentState())) {
					return;
				}
			}

			stateChanged = false;
			
			currentStateLock.writeLock().lock();
			try {
				oldState = currentState;
				currentState = Task.State.FINISHED;
				stateChanged = true;
			} finally {
				currentStateLock.writeLock().unlock();
			}
			
			if (stateChanged) {
				notifyObservers(oldState, currentState);
			}
			
		} catch (InterruptedException e) {
			throw new RuntimeException("Interrupted!", e);
		}
	}
	
	@Override
	public void requestPause() {

		boolean stateChanged = false;
		Task.State oldState = null;

		currentStateLock.writeLock().lock();
		try {
			oldState = currentState;
			currentState = Task.State.PAUSING;
			stateChanged = true;
		} finally {
			currentStateLock.writeLock().unlock();
		}

		if (stateChanged) {
			notifyObservers(oldState, currentState);
		}
	}

	@Override
	public void requestStop() {

		boolean stateChanged = false;
		Task.State oldState = null;

		currentStateLock.writeLock().lock();
		try {
			oldState = currentState;
			currentState = Task.State.STOPPING;
			stateChanged = true;
		} finally {
			currentStateLock.writeLock().unlock();
		}

		if (stateChanged) {
			notifyObservers(oldState, currentState);
		}
	}

	@Override
	public void requestResume() {

		boolean stateChanged = false;
		Task.State oldState = null;

		currentStateLock.writeLock().lock();
		try {
			oldState = currentState;
			currentState = Task.State.RUNNING;
			stateChanged = true;
		} finally {
			currentStateLock.writeLock().unlock();
		}
		
		if (stateChanged) {
			notifyObservers(oldState, currentState);
		}

		synchronized (this) {
			this.notify();
		}
	}

	@Override
	public void addStateObserver(StateObserver observer) {
		stateObservers.add(observer);
	}

	@Override
	public Task.State getCurrentState() {
		currentStateLock.readLock().lock();
		try {
			return currentState;
		} finally {
			currentStateLock.readLock().unlock();
		}
	}

	@Override
	public String toString() {
		return "Task [" + getName() + "]: state=" + currentState;
	}

}