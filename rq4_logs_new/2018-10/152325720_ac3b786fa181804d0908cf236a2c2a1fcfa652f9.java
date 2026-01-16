package ktu.tanks;

public class GameTicker {

    private boolean isRunning;
    private Tickable tickable;
    private long interval;

    private Runnable runnable = () -> {
        while (isRunning) {
            tickable.tick();
            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    };

    public GameTicker(Tickable tickable, long interval) {
        this.tickable = tickable;
        this.interval = interval;
    }

    public void start() {
        this.isRunning = true;
        new Thread(runnable).start();
    }

    public void stop() {
        this.isRunning = false;
    }
}