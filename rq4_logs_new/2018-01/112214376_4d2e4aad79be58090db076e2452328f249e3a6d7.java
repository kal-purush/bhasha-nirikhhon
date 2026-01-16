package strategy.cenes.choose;

import strategy.cenes.story.BaseStory;
import strategy.model.GameConfig;

public class ConfigChoose extends BaseChoose {

    final String STORY_PLAY = "1";
    final String OTHER = "2";

    @Override
    public void init() {
        allowInputs.add(STORY_PLAY);
        allowInputs.add(OTHER);
        userInterface.showTip("遊戲設定");
    }

    @Override
    public void showTip() {
        userInterface.showTip("(1)故事播放 (2)其他 (尚未開放)");
    }

    @Override
    public void options(String inputData) {
        switch (inputData) {
            case STORY_PLAY:
                new ConfigStoryPlayChoose().readyToChoose();
                choose();
//                setNextScenes(new ConfigStoryPlayChoose());
                break;
            case OTHER:
                break;
        }
    }

    @Override
    public void gotoNextScenes() {
//        setNextScenes(new MainChoose());
    }

    @Override
    public void gotoPreScenes() {

    }
}