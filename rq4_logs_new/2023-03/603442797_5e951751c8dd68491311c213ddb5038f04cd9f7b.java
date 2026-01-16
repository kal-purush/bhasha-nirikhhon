package homework.day7.stringtask;

public class Log {

    public void LogPrint(String logs) {
        String ipAddresses = "access_log.2020.09.07 212.168.101.5 granted\n" +
                "access_log.2020.09.07 212.168.101.6 denied\n" +
                "access_log.2020.09.07 212.168.101.6 denied\n" +
                "access_log.2020.09.07 212.168.122.6 denied";

        int[] okAmount = new int[10];
        int[] failedAmount = new int[10];
        String[] addresses = new String[10];

        int count = 0;
        for (String line : ipAddresses.split("\n")) {
            if (line == null) {
                continue;
            }

            String[] parts = line.split("\\s+", 3);
            String address = parts[1];
            String status = parts[2];

            boolean found = false;
            for (int i = 0; i < count; i++) {
                if (addresses[i].equals(address)) {
                    found = true;
                    if (status.equals("granted")) {
                        okAmount[i]++;
                    } else if (status.equals("denied")) {
                        failedAmount[i]++;
                    }
                    break;
                }
            }
            if (!found) {
                addresses[count] = address;
                if (status.equals("granted")) {
                    okAmount[count] = 1;
                } else if (status.equals("denied")) {
                    failedAmount[count] = 1;
                }
                count++;
            }
        }

        for (int i = 0; i < count; i++) {
            String ip = addresses[i];
            int okCount = okAmount[i];
            int failedCount = failedAmount[i];
            System.out.println("ip " + ip + ": ok - " + okCount + ", failed - " + failedCount);
        }
    }
}