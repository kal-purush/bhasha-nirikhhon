package net.sparkzz.servercontrol.config;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import net.sparkzz.servercontrol.ServerControl;
import ninja.leaping.configurate.ConfigurationNode;
import ninja.leaping.configurate.loader.ConfigurationLoader;
import org.spongepowered.api.event.Subscribe;
import org.spongepowered.api.event.state.ServerStartingEvent;
import org.spongepowered.api.event.state.ServerStoppingEvent;
import org.spongepowered.api.service.config.DefaultConfig;

import java.io.File;
import java.io.IOException;

/**
 * @author Brendon
 * @since April 30, 2015
 *
 * Thanks to zml2008 for the config manager!
 */
public class Config {

	@Inject
	@DefaultConfig(sharedRoot = true)
	private ConfigurationLoader loader;

	@Inject
	@DefaultConfig(sharedRoot = true)
	private File defaultConfig;

	private ConfigurationNode config = null;

	@Subscribe
	public void onServerStart(ServerStartingEvent event) {
		try {
			if (!defaultConfig.exists()) {
				defaultConfig.createNewFile();
				config = loader.load();

				// default config stuff
				save();
			}
		} catch (IOException exception) {
			ServerControl.getLogger().error("The default configuration could not be loaded or created!");
		}
	}

	public ConfigurationNode getConfig() {
		return config;
	}

	public void setValue(String node, Object value) {
		config.getNode(node).setValue(value);
	}

	public Optional<Object> getValue(String node) {
		Object value = config.getValue(node);

		if (value != null) return Optional.of(value);
		else return null;
	}

	public void reload() {
		try {
			config = loader.load();
		} catch (IOException exception) {
			ServerControl.getLogger().warn("There was a problem reloading the configuration file!");
		}
	}

	public void save() {
		try {
			loader.save(config);
		} catch (IOException exception) {
			ServerControl.getLogger().warn("There was a problem saving the configuration file!");
		}
	}

	public void unload() {
		save();
		loader = null;
		defaultConfig = null;
		config = null;
	}
}