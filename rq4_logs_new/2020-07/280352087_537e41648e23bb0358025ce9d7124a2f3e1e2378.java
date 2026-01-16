package wastedgames.game;

import com.badlogic.gdx.Game;
import com.badlogic.gdx.Gdx;
import com.badlogic.gdx.Screen;
import com.badlogic.gdx.files.FileHandle;
import com.badlogic.gdx.graphics.OrthographicCamera;
import com.badlogic.gdx.graphics.g2d.SpriteBatch;
import com.badlogic.gdx.graphics.g2d.TextureAtlas;
import com.badlogic.gdx.scenes.scene2d.Stage;
import com.badlogic.gdx.scenes.scene2d.ui.Skin;
import com.badlogic.gdx.utils.ObjectMap;
import com.badlogic.gdx.utils.viewport.StretchViewport;
import com.badlogic.gdx.utils.viewport.Viewport;

import wastedgames.game.maintenance.ResourceLoader;

public class Main extends Game
{
    OrthographicCamera camera;
    SpriteBatch batch;
    Skin skin=new Skin();
    Stage stage;
    ObjectMap<String, Screen> GameScreens;

    public ObjectMap<String, Screen> getGameScreens() {
        return GameScreens;
    }

    public void setGameScreens(ObjectMap<String, Screen> gameScreens) {
        GameScreens = gameScreens;
    }

    @Override
    public void create()
    {
        ResourceLoader.loadResources();
        batch = new SpriteBatch();

        GameScreens=new ObjectMap<>();
        camera = new OrthographicCamera();
        camera.setToOrtho(false, Gdx.graphics.getWidth(),  Gdx.graphics.getHeight());

        stage=new Stage();
        FileHandle fileHandle = Gdx.files.internal("uiskin.json");
        FileHandle atlasFile = fileHandle.sibling("uiskin.atlas");
        skin.addRegions(new TextureAtlas(atlasFile));
        skin.load(Gdx.files.internal("uiskin.json"));
        GameScreens.put("Field",new GameField(skin,batch,camera,this,stage));
        GameScreens.put("Technologies",new ResearchWindow(camera,batch,skin,stage,this));
        setScreen(GameScreens.get("Field"));

        //setScreen(new ResearchWindow(camera,batch,skin));
    }
    @Override
    public  void render()
    {
        super.render();
    }

    @Override
    public void dispose()
    {
        batch.dispose();
        ResourceLoader.dispose();
    }
}