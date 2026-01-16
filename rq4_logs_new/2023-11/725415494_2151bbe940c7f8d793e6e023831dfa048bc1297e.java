import java.util.Random;

public class App {
    public static void main(String[] args) {
        JobQueue j = getRandomJobQueue();
        j.resolveAll();
    }

    public static JobQueue getRandomJobQueue(){
        Random rng = new Random();
        Job[] newJobs = new Job[20];

        for (int i = 0; i < newJobs.length; i++) {
            newJobs[i] = new Job(rng.nextFloat());
        }
        return new JobQueue(newJobs);
    }

    public static JobQueue getTestJobQueue(){

        Job[] newJobs = new Job[]{
                new Job(0.0f),
                new Job(0.1f),
                new Job(0.2f),
                new Job(0.3f),
                new Job(0.4f),
                new Job(0.5f),
                new Job(0.6f),
                new Job(0.7f),
                new Job(0.8f),
                new Job(0.9f)
        };

        return new JobQueue(newJobs);
    }
}