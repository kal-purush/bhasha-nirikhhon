package File_work.Branch;



import java.io.File;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentSkipListMap;

class Adder implements Runnable {
    static volatile Map<String, Long> table = new ConcurrentSkipListMap<>();
    private static final String path = "src//File_work";
    static volatile ArrayList<String> arrayList = new ArrayList<>();


    @Override
    public void run() {
        File dir = new File(path);
        for (File f : Objects.requireNonNull(dir.listFiles())) {
            if (!f.isDirectory()) {
                table.put(f.getName(), f.length());
                arrayList.add(f.getName());
                System.out.println(f.getName() + "   " + f.length());


            }
        }

    }
}

class Average implements Runnable {
    Adder adder;
    Long l;

    public Average(Adder adder) {
        this.adder = adder;
        l = 1L;
    }

    @Override
    public void run() {
        try {
            Long sum = 1L;

            for (int i = 0; i < Adder.arrayList.size(); i++) {
                sum += Adder.table.get(Adder.arrayList.get(i));
            }
            System.out.println(sum / Adder.arrayList.size());
            l = sum;
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}

public class task_3 {
    public static void main(String[] args) throws InterruptedException {
        Adder ad = new Adder();
        Average av = new Average(ad);
        Thread adt = new Thread(ad);
        Thread avt = new Thread(av);
        adt.start();
        adt.join();
        avt.start();
        avt.join();
        System.out.println(av.l);
    }

}