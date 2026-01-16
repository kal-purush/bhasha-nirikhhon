package frc.robot.commands;

public abstract class Command extends Thread {
    private int period;
    private boolean verbose;
    
    public Command(String name, int periodInMillis, boolean verbose) {
        super(name);
        this.period = periodInMillis;
        this.verbose = verbose;
    }
    public Command(String name, int periodInMillis) {
        this(name, periodInMillis, false);
    }
    protected abstract void whileRunning();
    protected abstract boolean isFinished();
    private void onStart() {
        if(verbose) {
            System.out.println(getName() + "_STARTING*********");
        }
    }
    private void onFinish() {
        if(verbose) {
            System.out.println(getName() + "_FINISHED***********");
        }
    }
    public void run() {
        onStart();
        while(!isFinished()) {
            whileRunning();
            if(verbose) {
                System.out.print(getName() + " state: ");
                System.out.println(getState());
            }
            try {
                Thread.sleep(period);
            }
            catch(InterruptedException e) {
                System.out.println(getName() + " INTERRUPTED*********");
            }
        }
        onFinish();
    }
}