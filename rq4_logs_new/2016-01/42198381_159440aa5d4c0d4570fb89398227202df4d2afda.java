package build._10second;

import build._10second.cloudflare.CloudFlareClient;
import build._10second.cloudflare.Zone;
import build._10second.packet.Device;
import build._10second.packet.IpAddress;
import build._10second.packet.PacketClient;
import build._10second.packet.Project;
import org.junit.Ignore;
import org.junit.Test;

import java.math.BigDecimal;

import static com.googlecode.totallylazy.Assert.assertFalse;
import static com.googlecode.totallylazy.Maps.map;

public class Script {
    String domain = "10second.build";
    String hostname = "agent." + domain;

    @Test
    @Ignore("Manual")
    public void create() throws Exception {
        PacketClient packet = new PacketClient();
        Project project = packet.project(domain);
        assertFalse(packet.devices(project).exists(d -> d.hostname.equals(hostname)));

        Device provisioned = packet.provisionDevice(project, map(
                "hostname", hostname,
                "plan", "baremetal_0",
                "facility", "ewr1",
                "operating_system", "ubuntu_14_04"
        ));

        Device active = packet.pollUntil(provisioned, d -> d.state.equals("active"));

        IpAddress publicIp4 = active.ipAddresses().find(ip -> ip.address_family.equals(new BigDecimal("4")) && ip.Public).value();
        IpAddress publicIp6 = active.ipAddresses().find(ip -> ip.address_family.equals(new BigDecimal("6")) && ip.Public).value();

        final CloudFlareClient cloudFlare = new CloudFlareClient();
        Zone zone = cloudFlare.zone(this.domain);
        cloudFlare.createDnsRecord(zone, map("type", "A", "name", hostname, "content", publicIp4.address));
        cloudFlare.createDnsRecord(zone, map("type", "AAAA", "name", hostname, "content", publicIp6.address));
    }
}