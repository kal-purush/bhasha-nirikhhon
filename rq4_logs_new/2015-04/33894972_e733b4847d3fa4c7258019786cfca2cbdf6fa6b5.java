import java.awt.*;
import java.awt.image.BufferedImage;
import java.awt.image.ImageObserver;

/**
 * Created by SpaO on 2015-04-30.
 */

public class VisualEffect {

    private double x, y;
    private BufferedImage image = null;
    private int timeToShow;
    private int id;
    private static int counter = 0;

    public VisualEffect(final double x, final double y, VisualEffectType visualEffectType) {
        this.x = x;
        this.y = y;
        setValues(visualEffectType);
        this.timeToShow = 10;
        this.id = counter;
        increaseCounter();
    }

    private static void increaseCounter() {
        counter++;
    }

    public void drawVisualEffect(Graphics dbg, ImageObserver panel) {
        dbg.drawImage(image, (int) x, (int) y, panel);
    }

    private void setValues(VisualEffectType visualEffectType) {
        switch (visualEffectType) { // if we want to add more visualeffects
            case EXPLOSION:
                this.image = GamePanel.getImgExplosion();
                break;
            default:
        }
    }

    private void decreaseTimetoShow() {
        timeToShow--;
    }

    public void update(GamePanel gamePanel) {
        decreaseTimetoShow();
        if (timeToShow < 0) {
            gamePanel.addVisualEffectIdToRemove(id);
        }
    }

    public int getId() {
        return id;
    }
}