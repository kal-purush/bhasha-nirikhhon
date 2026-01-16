package Peer;

import java.io.File;
import java.net.DatagramPacket;
import java.net.MulticastSocket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class ReclaimProtocol {
    public static void run() {
        MulticastSocket controlSocket;
        DatagramPacket controlPacket;
        File file;
        byte[] buf;
        int savings;
        ArrayList<String[]> localChunkInfo, filter;

        try {
            controlSocket = new MulticastSocket(Peer.getMCport());
            controlSocket.joinGroup(Peer.getMCip());
            controlSocket.setLoopbackMode(true);
            controlSocket.setSoTimeout(100);
            localChunkInfo = Util.loadLocalChunkInfo();
            filter = new ArrayList<>();
            savings = 0;
            for (String[] chunk : localChunkInfo) {
                if (Integer.parseInt(chunk[4]) > Integer.parseInt(chunk[3])) {
                    filter.add(chunk);
                }
            }

            for (String[] chunk : filter) {
                try {
                    buf = buildHeader(chunk).getBytes(StandardCharsets.ISO_8859_1);
                    controlPacket = new DatagramPacket(buf, buf.length, Peer.getMCip(), Peer.getMCport());
                    controlSocket.send(controlPacket);
                    localChunkInfo.remove(chunk);
                    file = new File(chunk[0] + ".part" + chunk[1]);
                    file.delete();
                    savings += Integer.parseInt(chunk[2]);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            Util.saveLocalChunkInfo(localChunkInfo);
            controlSocket.close();
            System.out.println("ReclaimProtocol - Reclaimed " + savings + "B");
            System.out.println("ReclaimProtocol - Finished.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    static String buildHeader(String[] cmd) {
        return "REMOVED 1.0 " + cmd[0] + " " + cmd[1] + " \r\n\r\n";
    }
}