package first.core;

import playn.core.Image;
import playn.core.ImageLayer;
import playn.core.Layer;

import static playn.core.PlayN.assets;
import static playn.core.PlayN.graphics;

/**
 * Created by Berdniky on 07.01.2015.
 */
public class TankShell {
    Image image = assets().getImage("images/shell.png");
    ImageLayer layer = graphics().createImageLayer();
    float x, y, angle;
    private static final float SPEED = 4;
    private float updateCount;
    private static final float UPDATES_NUMBER_BEFORE_SHELL_REMOVING = 100;

    public TankShell(float x, float y, float angle) {
        this.x = x;
        this.y = y;
        this.angle = angle;

    }

    public Layer getLayer () {
        layer.setImage(image);
        layer.setOrigin(image.width() / 2f, image.height() / 2f);
        layer.setTranslation(x, y);
        layer.setRotation(angle);
        y -= SPEED * Math.cos(angle);
        x += SPEED * Math.sin(angle);
        updateCount++;
        return layer;
    }

    public boolean shellExist () {
        return updateCount != UPDATES_NUMBER_BEFORE_SHELL_REMOVING;
    }

}