package studio.jawa.bullettrain.screens.uiscreens;

import com.badlogic.gdx.Game;
import com.badlogic.gdx.Gdx;
import com.badlogic.gdx.Screen;
import com.badlogic.gdx.assets.AssetManager;
import com.badlogic.gdx.graphics.GL20;
import com.badlogic.gdx.graphics.Texture;
import com.badlogic.gdx.graphics.g2d.TextureRegion;
import com.badlogic.gdx.scenes.scene2d.InputEvent;
import com.badlogic.gdx.scenes.scene2d.Stage;
import com.badlogic.gdx.scenes.scene2d.ui.*;
import com.badlogic.gdx.scenes.scene2d.utils.ClickListener;
import com.badlogic.gdx.scenes.scene2d.utils.TextureRegionDrawable;
import com.badlogic.gdx.utils.viewport.ScreenViewport;

public class CharacterSelectScreen implements Screen {
    private final Game game;
    private final AssetManager assetManager;
    private Stage stage;
    private Skin skin;

    private Image portrait;
    private Label nameLabel;
    private Label info1Label;
    private Label info2Label;

    private CharacterInfo[] characters;
    private CharacterInfo selectedCharacter;

    public CharacterSelectScreen(Game game, AssetManager assetManager) {
        this.game = game;
        this.assetManager = assetManager;
    }

    @Override
    public void show() {
        stage = new Stage(new ScreenViewport());
        Gdx.input.setInputProcessor(stage);
        skin = assetManager.get("ui/uiskin.json", Skin.class);

        characters = new CharacterInfo[] {
            new CharacterInfo("Grace", assetManager.get("grace.png", Texture.class), "Daughter of a Raider warlord", "Glass canon"),
            new CharacterInfo("Jing Wei", assetManager.get("jing_wei.png", Texture.class), "Guardian Automata", "Quick dashes")
        };
        selectedCharacter = characters[0];

        Image bgBar = new Image(skin.newDrawable("white", 0.2f, 0.2f, 0.2f, 1f));
        bgBar.setSize(Gdx.graphics.getWidth(), 220);
        bgBar.setPosition(0, 0);
        stage.addActor(bgBar);

        Table root = new Table();
        root.setFillParent(true);
        root.pad(20);
        stage.addActor(root);

        Label infoLabel = new Label("Select your character", skin);
        infoLabel.setFontScale(5);
        root.top().padTop(40);
        root.add(infoLabel).expandX().top().padBottom(20).row();

        Table bottomContainer = new Table();
        bottomContainer.setFillParent(true);
        bottomContainer.bottom().padBottom(30).left().padLeft(30);
        stage.addActor(bottomContainer);

        portrait = new Image(new TextureRegionDrawable(characters[0].portrait));
        bottomContainer.add(portrait).width(500).height(500).left().padLeft(30).bottom().padBottom(30);

        Table rightContainer = new Table();

        nameLabel = new Label(characters[0].name, skin);
        nameLabel.setFontScale(3);
        info1Label = new Label("• " + characters[0].info1, skin);
        info1Label.setFontScale(2);
        info2Label = new Label("• " + characters[0].info2, skin);
        info2Label.setFontScale(2);

        rightContainer.add(nameLabel).padBottom(50).row();
        rightContainer.add(info1Label).padBottom(10).row();
        rightContainer.add(info2Label).padBottom(30).row();

        Table selectionTable = new Table();
        for (CharacterInfo character : characters) {
            TextButton charButton = new TextButton(character.name, skin);
            charButton.getLabel().setFontScale(1.2f);
            charButton.addListener(new ClickListener() {
                @Override
                public void clicked(InputEvent event, float x, float y) {
                    updateCharacterInfo(character);
                    selectedCharacter = character;
                }
            });
            selectionTable.add(charButton).width(180).height(60).pad(10);
        }

        TextButton confirmButton = new TextButton("Confirm", skin);
        confirmButton.getLabel().setFontScale(1.5f);
        confirmButton.addListener(new ClickListener() {
            @Override
            public void clicked(InputEvent event, float x, float y) {
                if (selectedCharacter != null) {
                    game.setScreen(new GameScreen(game, selectedCharacter));
                }
            }
        });

        rightContainer.add(selectionTable).padBottom(20).row();
        rightContainer.add(confirmButton).center().width(200).height(60);

        bottomContainer.add(rightContainer).expandX().bottom().right().padRight(30);
        root.add(bottomContainer).expand().fill().bottom().row();
    };

    public void updateCharacterInfo(CharacterInfo character) {
        portrait.setDrawable(new TextureRegionDrawable(new TextureRegion(character.portrait)));
        nameLabel.setText(character.name);
        info1Label.setText("• " + character.info1);
        info2Label.setText("• " + character.info2);
    }

    @Override
    public void render(float delta) {
        Gdx.gl.glClearColor(0, 0, 0.1f, 1);
        Gdx.gl.glClear(GL20.GL_COLOR_BUFFER_BIT);
        stage.act(delta);
        stage.draw();
    }

    @Override public void resize(int width, int height) { stage.getViewport().update(width, height, true); }
    @Override public void pause() {}
    @Override public void resume() {}
    @Override public void hide() {}
    @Override public void dispose() { stage.dispose(); }

    public static class CharacterInfo {
        public final String name;
        public final Texture portrait;
        public final String info1;
        public final String info2;

        public CharacterInfo(String name, Texture portrait, String info1, String info2) {
            this.name = name;
            this.portrait = portrait;
            this.info1 = info1;
            this.info2 = info2;
        }
    }
}