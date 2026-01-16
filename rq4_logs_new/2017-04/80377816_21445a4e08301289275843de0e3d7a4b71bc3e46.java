import org.jnetpcap.Pcap;
import org.jnetpcap.packet.JPacket;
import org.jnetpcap.packet.JPacketHandler;

import java.util.regex.Pattern;

/**
 * Created by Will on 4/12/2017.
 */
public class Driver
{
    private static class PacketIterator implements JPacketHandler<Ids[]>
    {
        @Override
        public void nextPacket(JPacket packet, Ids[] ids_array)
        {
            for (Ids ids : ids_array)
            {
                ids.handlePacket(packet);
            }
        }
    }

    public static void main(String[] args)
    {
        String path = args[0];

        StringBuilder errbuf = new StringBuilder();
        final Pcap pcap = Pcap.openOffline(path, errbuf);
        if (pcap == null)
        {
            System.err.println(errbuf);
            return;
        }

//        // Create a policy for the first part
//        StatelessPolicy policy = new StatelessPolicy();
//        policy.host_ip = "192.168.0.1";
//        policy.name = "Blame Attack 1";
//        policy.protocol = StatelessPolicy.Protocol.TCP;
//        policy.host_port = 110;
//
//        StatelessPolicy.SubPolicy sub_policy = new StatelessPolicy.SubPolicy();
//        sub_policy.type = Policy.SubPolicyType.FROM_HOST;
//        sub_policy.pattern = Pattern.compile("OK.*\r\n");
//        policy.sub_policies.add(sub_policy);
//
//        sub_policy = new StatelessPolicy.SubPolicy();
//        sub_policy.type = Policy.SubPolicyType.TO_HOST;
//        sub_policy.pattern = Pattern.compile("USER .*\r\n");
//        policy.sub_policies.add(sub_policy);
//
//        sub_policy = new StatelessPolicy.SubPolicy();
//        sub_policy.type = Policy.SubPolicyType.FROM_HOST;
//        sub_policy.pattern = Pattern.compile("OK.*\r\n");
//        policy.sub_policies.add(sub_policy);
//
//        sub_policy = new StatelessPolicy.SubPolicy();
//        sub_policy.type = Policy.SubPolicyType.TO_HOST;
//        sub_policy.pattern = Pattern.compile("PASS.*\r\n");
//        policy.sub_policies.add(sub_policy);
//
//        sub_policy = new StatelessPolicy.SubPolicy();
//        sub_policy.type = Policy.SubPolicyType.FROM_HOST;
//        sub_policy.pattern = Pattern.compile("OK.*\r\n");
//        policy.sub_policies.add(sub_policy);

//        StatelessPolicy policy = new StatelessPolicy();
//        policy.host_ip = "192.168.0.1";
//        policy.name = "TFTP Attacker boot";
//        policy.protocol = StatelessPolicy.Protocol.UDP;
//        policy.attacker_port = 69;
//
//        StatelessPolicy.SubPolicy sub_policy = new StatelessPolicy.SubPolicy();
//        sub_policy.type = Policy.SubPolicyType.FROM_HOST;
//        sub_policy.pattern = Pattern.compile("vmlinuz");
//        policy.sub_policies.add(sub_policy);
//
//        sub_policy = new StatelessPolicy.SubPolicy();
//        sub_policy.type = Policy.SubPolicyType.TO_HOST;
//        //sub_policy.pattern = Pattern.compile("\x00\x03\x00\x01");
//        policy.sub_policies.add(sub_policy);

        StatefulPolicy policy = new StatefulPolicy();
        policy.host_port = 5551;
        policy.type = Policy.SubPolicyType.TO_HOST;
        policy.regex = Pattern.compile("Now I own your computer");
        Ids[] ids = new Ids[]{ new StatefulIds(policy) };

        // Check the policy
        pcap.loop(Pcap.LOOP_INFINITE, new PacketIterator(), ids);

        // Close the file
        pcap.close();
    }
}