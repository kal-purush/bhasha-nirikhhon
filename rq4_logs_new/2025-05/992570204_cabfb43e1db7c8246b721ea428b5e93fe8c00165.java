package studio.jawa.bullettrain.screens.uiscreens;

import com.badlogic.gdx.Game;
import com.badlogic.gdx.Gdx;
import com.badlogic.gdx.Screen;
import com.badlogic.gdx.assets.AssetManager;
import com.badlogic.gdx.graphics.GL20;
import com.badlogic.gdx.scenes.scene2d.Stage;
import com.badlogic.gdx.scenes.scene2d.ui.Label;
import com.badlogic.gdx.scenes.scene2d.ui.ProgressBar;
import com.badlogic.gdx.scenes.scene2d.ui.Skin;
import com.badlogic.gdx.scenes.scene2d.ui.Table;
import com.badlogic.gdx.utils.viewport.ScreenViewport;

public class LoadingScreen implements Screen {
    private final Game game;
    private final AssetManager assetManager;

    private Stage stage;
    private Skin skin;
    private ProgressBar progressBar;
    private Label loadingLabel;

    public LoadingScreen(Game game, AssetManager assetManager) {
        this.game = game;
        this.assetManager = assetManager;
    }

    @Override
    public void show() {
        stage = new Stage(new ScreenViewport());
        Gdx.input.setInputProcessor(stage);
        skin = new Skin(Gdx.files.internal("ui/uiskin.json"));

        Table root = new Table();
        root.setFillParent(true);
        stage.addActor(root);

        loadingLabel = new Label("Loading...", skin);
        loadingLabel.setFontScale(2);

        progressBar = new ProgressBar(0f, 1f, 0.01f, false, skin);
        progressBar.setAnimateDuration(0.25f);
        progressBar.setWidth(300);

        root.add(loadingLabel).padBottom(20).row();
        root.add(progressBar).width(300).height(30);

        assetManager.load("ui/uiskin.json", Skin.class);

        assetManager.finishLoading();
    };

    @Override
    public void render(float delta) {
        Gdx.gl.glClearColor(0, 0, 0.15f, 1);
        Gdx.gl.glClear(GL20.GL_COLOR_BUFFER_BIT);

        if (assetManager.update()) {
            game.setScreen(new MainMenuScreen(game, assetManager));
        }

        progressBar.setValue(assetManager.getProgress());

        stage.act(delta);
        stage.draw();
    };

    @Override public void resize(int width, int height) { stage.getViewport().update(width, height, true); }
    @Override public void pause() {}
    @Override public void resume() {}
    @Override public void hide() {}
    @Override public void dispose() { stage.dispose(); skin.dispose(); }
}