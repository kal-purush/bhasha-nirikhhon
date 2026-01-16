package mygame;

import com.jme3.app.SimpleApplication;
import com.jme3.light.AmbientLight;
import com.jme3.light.DirectionalLight;
import static com.jme3.light.Light.Type.Ambient;
import com.jme3.material.Material;
import com.jme3.math.ColorRGBA;
import com.jme3.math.Vector3f;
import com.jme3.renderer.RenderManager;
import com.jme3.scene.Geometry;
import com.jme3.scene.Spatial;
import com.jme3.scene.shape.Box;
import com.jme3.system.AppSettings;


/**
 * This is the Main Class of your Game. You should only do initialization here.
 * Move your Logic into AppStates or Controls
 * @author normenhansen
 */
public class Main extends SimpleApplication {

    public static void main(String[] args) {
        Main app = new Main();
        
        app.showSettings = false;
        
        AppSettings ap = new AppSettings(true);
        ap.put("Widht", 640);
        ap.put("Height", 480);
        app.start();
    }

    @Override
    public void simpleInitApp() {
        
        this.flyCam.setEnabled(true);
        this.setDisplayFps(false);
        
        DirectionalLight ambient = new DirectionalLight();
        ambient.setDirection((new Vector3f(-0.5f,-0.5f,-0.5f)).normalizeLocal());
        ambient.setColor(ColorRGBA.White);
        rootNode.addLight(ambient);
        
        Spatial bm = assetManager.loadModel("Models/Ivan-model.blend");
        rootNode.attachChild(bm);
    }

    @Override
    public void simpleUpdate(float tpf) {
        //TODO: add update code
    }

    @Override
    public void simpleRender(RenderManager rm) {
        //TODO: add render code
    }
}