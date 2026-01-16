package com.bytebreakstudios.animagic;

import com.badlogic.gdx.math.Vector2;
import com.bytebreakstudios.animagic.texture.AnimagicTextureRegion;
import org.junit.Assert;
import org.junit.Test;

public class ShaderMultiTextureCoordinateEquationTest {

    @Test
    public void testSameTextureRegionSizes() {
        float width = 180;
        float height = 180;
        float x = 0;
        float y = 0;
        float w = 180;
        float h = 180;
        float width_n = 180;
        float height_n = 180;
        float x_n = 0;
        float y_n = 0;
        float w_n = 180;
        float h_n = 180;

        runEquations(buildData(width, height, x, y, w, h, width_n, height_n, x_n, y_n, w_n, h_n));
    }

    @Test
    public void testAbnormalTextureRegionSizes() {
        float width = 280;
        float height = 280;
        float x = 100;
        float y = 100;
        float w = 180;
        float h = 180;
        float width_n = 240;
        float height_n = 240;
        float x_n = 25;
        float y_n = 15;
        float w_n = 180;
        float h_n = 180;

        runEquations(buildData(width, height, x, y, w, h, width_n, height_n, x_n, y_n, w_n, h_n));
    }

    @Test
    public void testTextureRegionRandom() {
        float width = 100;
        float height = 100;
        float x = 20;
        float y = 20;
        float w = 25;
        float h = 25;
        float width_n = 75;
        float height_n = 75;
        float x_n = 10;
        float y_n = 10;
        float w_n = 20;
        float h_n = 20;

        runEquations(buildData(width, height, x, y, w, h, width_n, height_n, x_n, y_n, w_n, h_n));
    }

    public AnimagicTextureRegion.AnimagicTextureRegionShaderData buildData(float width, float height, float x, float y, float w, float h, float width_n, float height_n, float x_n, float y_n, float w_n, float h_n) {
        float xCoordMin = x / width;
        float xCoordMax = (x + w) / width;
        float xCoordDiff = xCoordMax - xCoordMin;

        float yCoordMin = y / height;
        float yCoordMax = (y + h) / height;
        float yCoordDiff = yCoordMax - yCoordMin;

        float xNorCoordMin = x_n / width_n;
        float xNorCoordMax = (x_n + w_n) / width_n;
        float xNorCoordDiff = xNorCoordMax - xNorCoordMin;

        float yNorCoordMin = y_n / height_n;
        float yNorCoordMax = (y_n + h_n) / height_n;
        float yNorCoordDiff = yNorCoordMax - yNorCoordMin;

        return new AnimagicTextureRegion.AnimagicTextureRegionShaderData(xCoordMin, xCoordDiff, yCoordMin, yCoordDiff, xNorCoordMin, xNorCoordDiff, yNorCoordMin, yNorCoordDiff);
    }

    public void runEquations(AnimagicTextureRegion.AnimagicTextureRegionShaderData d) {
        equation(d.xCoordMin, d.yCoordMin, d.xNorCoordMin, d.yNorCoordMin, d);
        equation(d.xCoordMin + d.xCoordDiff,
                d.yCoordMin + d.yCoordDiff,
                d.xNorCoordMin + d.xNorCoordDiff,
                d.yNorCoordMin + d.yNorCoordDiff, d);
    }

    public void equation(float inputX, float inputY, float expectedOutX, float expectedOutY, AnimagicTextureRegion.AnimagicTextureRegionShaderData d) {
        Vector2 result = new Vector2(
                d.xNorCoordMin + (((inputX - d.xCoordMin) / d.xCoordDiff) * d.xNorCoordDiff),
                d.yNorCoordMin + (((inputY - d.yCoordMin) / d.yCoordDiff) * d.yNorCoordDiff));
        Assert.assertEquals(expectedOutX, result.x, 0);
        Assert.assertEquals(expectedOutY, result.y, 0);
    }
}