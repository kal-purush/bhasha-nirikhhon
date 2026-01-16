package studio.jawa.bullettrain.screens.gamescreens;

import com.badlogic.ashley.core.Engine;
import com.badlogic.ashley.core.Entity;
import com.badlogic.gdx.Gdx;
import com.badlogic.gdx.Screen;
import com.badlogic.gdx.assets.AssetManager;
import com.badlogic.gdx.graphics.GL20;
import com.badlogic.gdx.graphics.OrthographicCamera;
import com.badlogic.gdx.graphics.Texture;
import com.badlogic.gdx.graphics.g2d.Sprite;
import studio.jawa.bullettrain.components.technicals.SpriteComponent;
import studio.jawa.bullettrain.components.technicals.TransformComponent;
import studio.jawa.bullettrain.systems.technicals.RenderingSystem;

/** First screen of the application. Displayed after the application is created. */
public class TestScreen implements Screen {
    private Engine engine;
    private OrthographicCamera camera;

    @Override
    public void show() {
        engine = new Engine();
        camera = new OrthographicCamera(Gdx.graphics.getWidth(), Gdx.graphics.getHeight());
        camera.position.set(camera.viewportWidth / 2, camera.viewportHeight / 2, 0);
        camera.update();

        engine.addSystem(new RenderingSystem(camera));
        AssetManager manager = new AssetManager();

        manager.load("testing/dummy.png", Texture.class);

        manager.finishLoading();

        Texture tex = manager.get("testing/dummy.png", Texture.class);

        // Example entity
        Sprite sprite = new Sprite(tex);
        Entity entity = new Entity();
        TransformComponent pos = new TransformComponent();
        pos.position.x = 50;
        pos.position.y = 50;
        entity.add(pos);
        entity.add(new SpriteComponent(sprite));

        engine.addEntity(entity);
    }

    @Override
    public void render(float delta) {
        Gdx.gl.glClearColor(0, 0, 0, 1);
        Gdx.gl.glClear(GL20.GL_COLOR_BUFFER_BIT);

        engine.update(delta);
    }

    @Override
    public void resize(int width, int height) {
        // If the window is minimized on a desktop (LWJGL3) platform, width and height are 0, which causes problems.
        // In that case, we don't resize anything, and wait for the window to be a normal size before updating.
        if(width <= 0 || height <= 0) return;

        // Resize your screen here. The parameters represent the new window size.
    }

    @Override
    public void pause() {
        // Invoked when your application is paused.
    }

    @Override
    public void resume() {
        // Invoked when your application is resumed after pause.
    }

    @Override
    public void hide() {
        // This method is called when another screen replaces this one.
    }

    @Override
    public void dispose() {
        // Destroy screen's assets here.
    }
}