package engine.render.guis;

import engine.render.guis.components.GuiComponent;
import engine.render.models.RawModel;
import engine.render.textures.Texture;
import engine.utils.Loader;
import org.joml.Matrix4f;

import java.util.ArrayList;

public class Gui {

    private static RawModel rect = Loader.loadToVAO(new float[]{0,1,0,1,1,0,0,0,0,1,0,0},new float[]{0,0,-1,0,0,-1,0,0,-1,0,0,-1},new float[]{0,0,1,0,0,1,1,1},new int[]{3,1,2,2,1,0});
    private float x, y, w, h;
    private Matrix4f transformationMatrix = new Matrix4f();
    private Texture texture;
    private ArrayList<GuiComponent> components = new ArrayList<>();

    public Gui(Texture texture, float x, float y, float w, float h) {
        this.texture = texture;
        this.x = x;
        this.y = y;
        this.w = w;
        this.h = h;
        calculateMatrix();
    }

    private void calculateMatrix() {
        transformationMatrix.identity();
        transformationMatrix.translate(x, y, 0);
        transformationMatrix.scale(w, h, 0);
    }

    public void addComponent(GuiComponent component){
        components.add(component);
    }

    public ArrayList<GuiComponent> getComponents() {
        return components;
    }

    public static RawModel getRect(){
        return rect;
    }

    public Matrix4f getTransformationMatrix() {
        return transformationMatrix;
    }

    public Texture getTexture() {
        return texture;
    }
}