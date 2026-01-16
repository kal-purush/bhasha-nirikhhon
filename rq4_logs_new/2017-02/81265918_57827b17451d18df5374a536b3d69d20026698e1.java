package assl.cs.usu.edu.energyprofiling;

/**
 * Created by ywkwon on 2/15/2017.
 */

public class Profiler {

    private static Profiler instance;

    private Profiler() {

    }

    public static Profiler getInstance() {
        if(instance == null) {
            instance = new Profiler();
        }
        return instance;
    }

    public void start() {
        //cal readCPUUUsage() periodically
    }

    public double stop() {
        //calculate energy consumption
        return 0;
    }

    private double readCPUUsage() {
        //read current CPU frequency
        return 0;
    }
}