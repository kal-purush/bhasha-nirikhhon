package com.mygdx.game;

import com.badlogic.gdx.assets.AssetManager;

public class AdvancedAssetManager extends AssetManager {

	/**This method ensures that the requested resource got loaded and returns a reference to it
	 * @return Returns a reference to the requested resource*/
	public <T> T easyget(String filename, Class<T> type) {
		if(!super.isLoaded(filename, type)) {
			super.load(filename, type);
			super.finishLoadingAsset(filename);
		}
		return super.get(filename, type);
	}
}