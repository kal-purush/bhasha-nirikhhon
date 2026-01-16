package testbench;


import bench.NewtonSquareRoot;
import bench.SquareRootRunnable;
import logging.ConsoleLogger;
import logging.ILogger;
import logging.TimeUnit;
import timing.ITimer;
import timing.Timer;

public class TestSquareRoot {
    public static void main(String args[]) {
        ITimer timer = new Timer();
        ILogger log = new ConsoleLogger();
        int nr_cores=Runtime.getRuntime().availableProcessors();
        NewtonSquareRoot bench = new NewtonSquareRoot();

        bench.initialize(nr_cores);
        timer.start();
        bench.run();
        long time = timer.stop();

        TimeUnit timeUnit = TimeUnit.Sec;
        log.writeTime("Finished in ", time, timeUnit);
    }
}