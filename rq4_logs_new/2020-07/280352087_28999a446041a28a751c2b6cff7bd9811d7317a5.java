package wastedgames.game.card;




import com.badlogic.gdx.scenes.scene2d.ui.Button;
import com.badlogic.gdx.scenes.scene2d.ui.Dialog;
import com.badlogic.gdx.scenes.scene2d.ui.Skin;

import javax.naming.Context;

public class Card extends Dialog
{
    {
        text("1234567890134235251\n 1  1231   ");
        button("Yes","Yes");
        button("No","No");
    }



    public Card(String title, Skin skin) {
        super(title, skin);
    }

    public Card(String title, Skin skin, String windowStyleName) {
        super(title, skin, windowStyleName);
    }

    public Card(String title, WindowStyle windowStyle) {
        super(title, windowStyle);
    }
}