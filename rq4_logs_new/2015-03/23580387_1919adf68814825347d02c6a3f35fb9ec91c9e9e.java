package info.ccserver.homunculus.core;

import info.ccserver.homunculus.common.entity.EntityHomunculus;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;

public class HomunculusManager {
	/** players in the current instance */
	private final List<EntityHomunculus> homunculi = Lists.newArrayList();
	private final Map<EntityHomunculus, Environment> environments = new MapMaker()
			.makeMap();

	public void addHomunculus(EntityHomunculus entity) {

		// TODO チャンクロードとか実装する必要があるかどうか検証
		this.homunculi.add(entity);
	}

	public void removeHomunculus(EntityHomunculus entity) {

		this.homunculi.remove(entity);
	}
}