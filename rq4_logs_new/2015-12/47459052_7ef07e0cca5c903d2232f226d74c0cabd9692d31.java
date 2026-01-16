package data.scripts.world.freespace;

import com.fs.starfarer.api.Global;
import com.fs.starfarer.api.campaign.*;
import com.fs.starfarer.api.impl.campaign.ids.Conditions;
import com.fs.starfarer.api.impl.campaign.ids.Submarkets;

import java.awt.*;
import java.util.ArrayList;
import java.util.Arrays;

public class Aldebaran {

    public void generate(SectorAPI sector) {
        StarSystemAPI system = sector.createStarSystem("Aldebaran");
        system.getLocation().set(8400, -10750);
        system.setBackgroundTextureFilename("graphics/backgrounds/background4.jpg");

        PlanetAPI aldebaran = system.initStar("aldebaran", // unique id for this star
                "star_blue", // id in planets.json
                700f,		// radius (in pixels at default zoom)
                800, // corona radius, from star edge
                15f, // solar wind burn level
                1f, // flare probability
                4f); // cr loss mult

        system.setLightColor(new Color(100, 100, 255)); // light color in entire system, affects all entities

        PlanetAPI newvasuda = system.addPlanet("newvasuda", aldebaran, "New Vasuda", "arid", 300, 200, 3400, 215);
        newvasuda.getSpec().setGlowTexture(Global.getSettings().getSpriteName("hab_glows", "volturn"));
        newvasuda.getSpec().setGlowColor(new Color(255,255,255,255));
        newvasuda.getSpec().setUseReverseLightForGlow(true);
        newvasuda.applySpecChanges();
        newvasuda.setCustomDescriptionId("fs_newvasuda");

        // Vasudan Station
        SectorEntityToken vasudanStation = system.addCustomEntity("fs_vasuda_base",
                "Vasudan Naval Command", "station_jangala_type", "fs_vasudan");
        vasudanStation.setCircularOrbitPointingDown(system.getEntityById("newvasuda"), 45 + 180, 300, 50);
        vasudanStation.setCustomDescriptionId("fs_vasuda_base");

        // New Vasuda Jumppoint
        JumpPointAPI newvasuda_jumppoint = Global.getFactory().createJumpPoint("newvasuda_jump", "New Vasuda Jumppoint");
        newvasuda_jumppoint.setCircularOrbit(system.getEntityById("newvasuda"), 300+100, 3400, 215);
        newvasuda_jumppoint.setRelatedPlanet(newvasuda);

        newvasuda_jumppoint.setStandardWormholeToHyperspaceVisual();
        system.addEntity(newvasuda_jumppoint);

        PlanetAPI apep = system.addPlanet("apep", aldebaran, "Apep", "frozen", 200, 150, 6000, 400);
        apep.getSpec().setGlowColor(new Color(255, 255, 255, 255));
        apep.getSpec().setUseReverseLightForGlow(true);
        apep.applySpecChanges();
        apep.setCustomDescriptionId("fs_apep");

        // Hammer of Light Station
        SectorEntityToken holStation = system.addCustomEntity("fs_apep_base",
                "Hammer of Light Base", "station_pirate_type", "fs_hammeroflight");
        holStation.setCircularOrbitPointingDown(system.getEntityById("apep"), 45, 300, 50);
        holStation.setCustomDescriptionId("fs_apep_base");
        holStation.setInteractionImage("illustrations", "pirate_station");

        // Apep Jumppoint
        JumpPointAPI apep_jumppoint = Global.getFactory().createJumpPoint("apep_jump", "Apep Jumppoint");
        apep_jumppoint.setCircularOrbit(system.getEntityById("apep"), 200+60, 6000, 400);
        apep_jumppoint.setRelatedPlanet(apep);

        apep_jumppoint.setStandardWormholeToHyperspaceVisual();
        system.addEntity(apep_jumppoint);

        addMarketplace.addMarketplace("fs_vasudan", newvasuda,
                new ArrayList<>(Arrays.asList(vasudanStation)),
                "New Vasuda",
                7,
                new ArrayList<>(Arrays.asList(Conditions.ARID, Conditions.AUTOFAC_HEAVY_INDUSTRY, Conditions.MILITARY_BASE,
                        Conditions.POPULATION_7, Conditions.HEADQUARTERS,
                        Conditions.SPACEPORT, Conditions.ORE_COMPLEX, Conditions.ORE_REFINING_COMPLEX)),
                new ArrayList<>(Arrays.asList(Submarkets.SUBMARKET_STORAGE, Submarkets.GENERIC_MILITARY,
                        Submarkets.SUBMARKET_OPEN, Submarkets.SUBMARKET_BLACK)),
                0.3f
        );

        addMarketplace.addMarketplace("fs_hammeroflight", apep,
                new ArrayList<>(Arrays.asList(holStation)),
                "New Vasuda",
                5,
                new ArrayList<>(Arrays.asList(Conditions.ICE, Conditions.AUTOFAC_HEAVY_INDUSTRY, Conditions.MILITARY_BASE,
                        Conditions.POPULATION_5, Conditions.HEADQUARTERS,
                        Conditions.SPACEPORT, Conditions.ORE_COMPLEX, Conditions.ORE_REFINING_COMPLEX)),
                new ArrayList<>(Arrays.asList(Submarkets.SUBMARKET_STORAGE, Submarkets.GENERIC_MILITARY,
                        Submarkets.SUBMARKET_OPEN, Submarkets.SUBMARKET_BLACK)),
                0.3f
        );

        system.autogenerateHyperspaceJumpPoints(true, true);
    }
}