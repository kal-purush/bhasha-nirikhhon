package data.scripts;

import com.fs.starfarer.api.BaseModPlugin;
import com.fs.starfarer.api.Global;
import com.fs.starfarer.api.PluginPick;
import com.fs.starfarer.api.campaign.CampaignPlugin;
import com.fs.starfarer.api.combat.MissileAPI;
import com.fs.starfarer.api.combat.ShipAPI;
import org.dark.shaders.light.LightData;
import org.dark.shaders.util.ShaderLib;
import org.dark.shaders.util.TextureData;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class FreespaceModPlugin extends BaseModPlugin {

    @Override
    public void onApplicationLoad() {
        ShaderLib.init();
        LightData.readLightDataCSV("data/lights/fs_light_data.csv");
        TextureData.readTextureDataCSV("data/lights/fs_texture_data.csv");
    }
}