package engine;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import javax.sound.sampled.Clip;
import java.io.File;

import static org.junit.jupiter.api.Assertions.*;

class MusicManagerTest {

    MusicManager m;

    @BeforeEach
    void setUp() {
        m = new MusicManager();
    }


    @Test
    void getIsMute() {
        assertFalse(m.getIsMute());
        System.out.println("Test 1: getIsMute() \t# (exp) false \t\t\t\t(test) "+m.getIsMute());
    }

    @Test
    void toggleIsMute() {
        assertFalse(m.getIsMute());
        System.out.println("Test 2: before toggle \t# (exp) false \t\t\t\t(test) "+m.getIsMute());
        m.toggleIsMute();
        assertTrue(m.getIsMute());
        System.out.println("Test 3: after toggle \t# (exp) true \t\t\t\t(test) "+m.getIsMute());
    }

    @Test
    void getBgm() {
        File gameBgmFile = new File("res/GameBGM.wav");
        File testGameBgmFile = m.getBgm(MusicManager.BgmType.GameBgm);
        assertEquals(gameBgmFile, testGameBgmFile);
        System.out.println("Test 4: gameBgmFile \t# (exp) "+ gameBgmFile +" \t(test) "+testGameBgmFile);
    }

}