package systems.reformcloud.reformcloud2.prefixes;

import org.bukkit.Bukkit;
import org.bukkit.plugin.java.JavaPlugin;
import systems.reformcloud.reformcloud2.executor.api.common.ExecutorAPI;
import systems.reformcloud.reformcloud2.prefixes.listener.BukkitPrefixListener;
import systems.reformcloud.reformcloud2.prefixes.listener.ReformCloudPrefixListener;

public class ReformCloud2Prefixes extends JavaPlugin {

    private static ReformCloud2Prefixes instance;

    @Override
    public void onLoad() {
        instance = this;
    }

    @Override
    public void onEnable() {
        Bukkit.getPluginManager().registerEvents(new BukkitPrefixListener(), this);
        ExecutorAPI.getInstance().getEventManager().registerListener(new ReformCloudPrefixListener());
    }

    public static ReformCloud2Prefixes getInstance() {
        return instance;
    }
}