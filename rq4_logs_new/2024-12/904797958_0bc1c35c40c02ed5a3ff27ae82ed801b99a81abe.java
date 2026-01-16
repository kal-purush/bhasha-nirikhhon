package org.example;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

class TankControlSystemTest {

    @Test
    void testPumpTurnsOnAtMinLevel() {
        TankControlSystem system = new TankControlSystem(30.0);
        system.openFillValve();
        system.controlSystem();

        assertTrue(system.isPump(), "Pump should turn ON at 30% level");
    }

    @Test
    void testPumpTurnsOffAtMaxLevel() {
        TankControlSystem system = new TankControlSystem(100.0);
        system.openFillValve();
        system.controlSystem();

        assertFalse(system.isPump(), "Pump should turn OFF at 100% level");
    }

    @Test
    void testValveOutOpensOnDrainButton() {
        TankControlSystem system = new TankControlSystem(50.0);
        system.setDrainBottom(true);
        system.controlSystem();

        assertTrue(system.isValveOut(), "Valve V2 should open when drain button is pressed");
        assertFalse(system.isValveIn(), "Valve V1 should close when drain button is pressed");
    }

    @Test
    void testSimulateStepPumpFillsTank() {
        TankControlSystem system = new TankControlSystem(30.0);
        system.openFillValve();
        system.controlSystem();

        system.simulateStep(5.0);

        assertEquals(35.0, system.getTankLevel(), 0.1, "Tank level should increase by 5%");
    }

    @Test
    void testSimulateStepValveOutDrainsTank() {
        TankControlSystem system = new TankControlSystem(50.0);
        system.setDrainBottom(true);
        system.controlSystem();

        system.simulateStep(10.0);

        assertEquals(40.0, system.getTankLevel(), 0.1, "Tank level should decrease by 10%");
    }

}