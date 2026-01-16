package com.lohika.morning.ecs.vaadin;

import com.vaadin.shared.Registration;
import com.vaadin.ui.Component;
import com.vaadin.ui.Window;
import lombok.SneakyThrows;

import java.io.Serializable;

public abstract class MorningPopup extends Window {
    private static final long serialVersionUID = 1L;

    public MorningPopup(String caption) {
        super(caption);
        setModal(true);
        setClosable(false);
        setResizable(false);
        setDraggable(false);
        center();

    }

    @SneakyThrows(NoSuchMethodException.class)
    public Registration addHideListener(HideListener listener) {
        return addListener(HideEvent.class, listener,
                HideListener.class.getDeclaredMethod("windowHide", HideEvent.class));
    }

    public static class HideEvent extends Component.Event {
        public HideEvent(Component source) {
            super(source);
        }
    }

    @FunctionalInterface
    public interface HideListener extends Serializable {
        void windowHide(HideEvent hideEvent);
    }

    @Override
    public void setVisible(boolean visible) {
        boolean wasVisible = isVisible();
        super.setVisible(visible);
        if (wasVisible && !isVisible()) {
            fireEvent(new HideEvent(this));
        }
    }

    public void hide() {
        this.setVisible(false);
    }

    public void show() {
        this.setVisible(true);
    }
}