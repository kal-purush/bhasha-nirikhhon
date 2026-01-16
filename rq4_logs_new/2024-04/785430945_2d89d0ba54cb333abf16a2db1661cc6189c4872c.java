package simstation;

import java.io.Serializable;

public class Agent implements Runnable {
    private Simulation simulation;
    private Thread myThread;
    private String name;
    private Heading heading;
    private int xc, yc;
    private boolean suspended = false, stopped = false;

    public void setSimulation(Simulation s) {
        this.simulation = s;
    }

    @Override
    public void run() {
        myThread = Thread.currentThread();
        while (!stopped) {
            try {
                update();
                Thread.sleep(1000);
                checkSuspended();
            } catch(InterruptedException e) {
                // do nothing for now
            }
        }
    }

    public synchronized void stop() { stopped = true; }
    public synchronized boolean isStopped() { return stopped; }
    public synchronized void suspend() { suspended = true; }
    public synchronized boolean isSuspended() { return suspended;  }
    public synchronized void resume() { notify(); }

    private synchronized void checkSuspended() {
        try {
            while(!stopped && suspended) {
                wait();
                suspended = false;
            }
        } catch (InterruptedException e) {
            // do nothing for now
        }
    }

    public void move(int steps) {
        // will change the xc and yc of the agent
    }

    public void update() {
        // implemented in subclasses
    }
}