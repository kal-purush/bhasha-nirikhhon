package cudera.content;

import cudera.type.weather.BlizzardWeather;
import mindustry.type.Weather;
import mindustry.gen.Sounds;

public class CuderaWeathers {
    public static Weather blizzard;

    public static void load(){
        blizzard = new BlizzardWeather("blizzard"){{
            particleRegion = "particle";
            sizeMax = 14f;
            sizeMin = 3f;
            density = 600f;
            baseSpeed = 15f;
            yspeed = -2.5f;
            xspeed = 8f;
            minAlpha = 0.75f;
            maxAlpha = 0.9f;
            sound = Sounds.windhowl;
            soundVol = 0.25f;
            soundVolOscMag = 1.5f;
            soundVolOscScl = 1100f;
            soundVolMin = 0.15f;
        }};
    }
}