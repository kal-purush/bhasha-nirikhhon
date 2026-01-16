import aybici.parkourplugin.ParkourPlugin;
import be.seeseemelk.mockbukkit.MockBukkit;
import be.seeseemelk.mockbukkit.ServerMock;
import be.seeseemelk.mockbukkit.entity.PlayerMock;
import org.bukkit.Location;
import org.bukkit.Material;
import org.bukkit.World;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class VisualPlayerTimerTest {
    private ServerMock server;
    private PlayerMock player;
    private World world;

    private Location greenWoolBlockLocation;
    private Location redWoolBlockLocation;

    @Before
    public void setUp() {
        server = MockBukkit.mock();
        MockBukkit.load(ParkourPlugin.class);
        player = server.addPlayer();
        player.setOp(true);
        world = player.getWorld();

        greenWoolBlockLocation = new Location(world, 0, 6, 0);
        redWoolBlockLocation = new Location(world, 10, 6, 10);

        greenWoolBlockLocation.getBlock().setType(Material.LIME_WOOL);
        redWoolBlockLocation.getBlock().setType(Material.RED_WOOL);
        player.setLocation(new Location(world, 1,5,1));
        player.performCommand("apk add visualtest1");
    }

    @After
    public void tearDown() {
        MockBukkit.unmock();
    }

    //TODO: Wait for MockBukkit
    @Test
    public void visualTimerTest(){
        player.simulatePlayerMove(greenWoolBlockLocation.clone().add(0,1,0));
        server.getScheduler().performTicks(25);
        //assertEquals(1, player.getLevel());
    }
}