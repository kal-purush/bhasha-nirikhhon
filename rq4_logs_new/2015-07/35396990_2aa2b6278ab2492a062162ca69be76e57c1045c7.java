package com.alphasystem.morphologicalanalysis.ui.wordbyword.component;

import com.alphasystem.arabic.model.ArabicLetterType;
import javafx.beans.property.BooleanProperty;
import javafx.beans.property.ObjectProperty;
import javafx.beans.property.SimpleBooleanProperty;
import javafx.beans.property.SimpleObjectProperty;
import javafx.scene.layout.StackPane;
import javafx.scene.shape.Rectangle;
import javafx.scene.text.Text;

import static javafx.beans.binding.Bindings.when;
import static javafx.scene.paint.Color.*;
import static javafx.scene.text.Font.font;
import static javafx.scene.text.FontWeight.BOLD;

/**
 * @author sali
 */
public class RootLetterPane extends StackPane {

    private final ObjectProperty<ArabicLetterType> letter;
    private final BooleanProperty selected;

    public RootLetterPane() {
        setFocusTraversable(true);

        letter = new SimpleObjectProperty<>();
        selected = new SimpleBooleanProperty();

        final Rectangle background = new Rectangle(32, 32);
        background.setFill(TRANSPARENT);
        background.setStrokeWidth(1);
        background.setArcWidth(6);
        background.setArcHeight(6);
        background.setOnMouseClicked(event -> requestFocus());
        background.strokeProperty().bind(when(selectedProperty()).then(RED).otherwise(BLACK));
        setSelected(false);

        Text label = new Text();
        label.setFont(font("Traditional Arabic", BOLD, 20));
        label.setOnMouseClicked(event -> requestFocus());
        letterProperty().addListener((observable, oldValue, newValue) -> {
            String text = newValue == null ? "" : newValue.toUnicode();
            label.setText(text);
        });

        setLetter(null);
        getChildren().addAll(background, label);
    }

    public final void setSelected(boolean selected) {
        this.selected.set(selected);
    }

    public final BooleanProperty selectedProperty() {
        return selected;
    }

    public final void setLetter(ArabicLetterType letter) {
        this.letter.set(letter);
    }

    public final ObjectProperty<ArabicLetterType> letterProperty() {
        return letter;
    }

}