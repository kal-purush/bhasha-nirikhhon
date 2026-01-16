package pl.edu.agh.ki.sr;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class Main {
    private static final String znode = "/znode_testowy";

    /**
     * @param args
     */
    public static void main(String[] args) {
        if (args.length < 2) {
            System.err
                    .println("USAGE: Main hostPort program [args ...]");
            System.exit(2);
        }

        String hostPort = args[0];
        String exec[] = new String[args.length - 1];
        System.arraycopy(args, 1, exec, 0, exec.length);
        try {
            Executor executor = new Executor(hostPort, znode, exec);
            new Thread(() -> {
                try (BufferedReader br = new BufferedReader(new InputStreamReader(System.in))) {
                    while (true) {
                        switch (br.readLine()) {
                            case "tree":
                                executor.printDescendantsTree(znode);
                                break;
                        }
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }).start();
            executor.run();


        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}