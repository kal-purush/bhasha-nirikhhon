package ne.game.objects;
import org.newdawn.slick.*;
import org.newdawn.slick.tests.SoundTest;

import javax.swing.*;
import java.util.ArrayList;

public class EasyGame extends BasicGame {

    private ArrayList<MeinUfo> mUfoList;

    private Image background;

    private Crusher crusher;
    private Sound sound;
    private Music music;
    private int lautstärke = 0;
    private int hit = 1;
    private int miss = 1;
    private AngelCodeFont font;
    private Animation animation;

    public EasyGame() {
        super("EasyGame");
    }

    public static void main(String[] args) throws SlickException {
        AppGameContainer container = new AppGameContainer(new EasyGame());
        container.setDisplayMode(1024, 768, false);
        //container.setClearEachFrame(false);
        container.setMinimumLogicUpdateInterval(25);
        container.setTargetFrameRate(144);
        container.setShowFPS(true);
        container.start();
    }

    @Override
    public void init(GameContainer container) throws SlickException {
        animation = new Animation();
        PackedSpriteSheet pss = new PackedSpriteSheet("res/animation/player1.def");
        for (int i=1;i<=11;i++) {
            animation.addFrame(pss.getSprite("flame_" + i + ".png"), 100);
        }
            font = new AngelCodeFont("testdata/demo2.fnt","testdata/demo2_00.tga");
            background = new Image("assets/pics/background.png");
            mUfoList = new ArrayList<MeinUfo>();
                for (int i = 1; i <= 1000; i++) {
                    mUfoList add(new MeinUfo(100, 100, new Image("assets/pics/meinufo.png")));
                }
            crusher = new Crusher(512,700,new Image("assets/pics/crusher.png"),container.getInput());
            music = new Music("testdata/testloop.ogg");
            sound = new Sound("testdata/burp.aif");
            music.loop();

        }

    @Override
    public void update(GameContainer container, int delta) throws SlickException {
        Input input = container.getInput();

        if (input.isKeyPressed(Input.KEY_ESCAPE)) {
            container.exit();
        }
        if (input.isKeyPressed(Input.KEY_UP)){
            lautstärke = lautstärke +1;
            if (lautstärke >=10) lautstärke = 10;
            music.setVolume(lautstärke/10f);
        }
        if (input.isKeyPressed(Input.KEY_DOWN)) {
            lautstärke = lautstärke - 1;
            if (lautstärke < 1) lautstärke = 0;
            music.setVolume(lautstärke / 10f);
        }

        for (MeinUfo u : mUfoList) {
            if (crusher.intersects(u.getShape())) {
                System.out.println("collide");
                sound.play();
                u.setRandomPosition();
                hit++;
            }
            if (u.getY() > 768) {
                miss++;
                u.setRandomPosition();
            }
            u.update(delta);
        }
        crusher.update(delta);
    }

    @Override
    public void render(GameContainer container, Graphics g) throws SlickException {
        background.draw();
        for (MeinUfo u : mUfoList) {
            u.draw(g);
        }
        crusher.draw(g);
        font.drawString(8, 25, "Hit "+hit, Color.black);
        font.drawString(7, 50, "Miss "+miss, Color.red);
    }
}


