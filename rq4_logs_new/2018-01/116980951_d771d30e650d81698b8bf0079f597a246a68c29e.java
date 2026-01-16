package com.lohika.morning.ecs.domain.event;

import com.vaadin.data.Binder;
import com.vaadin.event.ShortcutAction;
import com.vaadin.icons.VaadinIcons;
import com.vaadin.shared.Registration;
import com.vaadin.spring.annotation.SpringComponent;
import com.vaadin.spring.annotation.UIScope;
import com.vaadin.ui.Button;
import com.vaadin.ui.Component;
import com.vaadin.ui.DateField;
import com.vaadin.ui.FormLayout;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.TextField;
import com.vaadin.ui.VerticalLayout;
import com.vaadin.ui.Window;
import com.vaadin.ui.themes.ValoTheme;
import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.Serializable;
import java.util.Optional;

@SpringComponent
@UIScope
public class EventEditor extends Window {
    private static final long serialVersionUID = 1L;

    private final EventRepository repository;

    /**
     * The currently edited event
     */
    private com.lohika.morning.ecs.domain.event.Event event;

    /* Fields to edit properties in Event entity */
    private TextField name = new TextField("Name");
    private TextField description = new TextField("Description");
    private DateField date = new DateField("Date");

    /* Action buttons */
    private Button save = new Button("Save", VaadinIcons.CHECK);
    private Button cancel = new Button("Cancel");
    private Button delete = new Button("Delete", VaadinIcons.TRASH);
    private HorizontalLayout actions = new HorizontalLayout(save, delete, cancel);

    private Binder<com.lohika.morning.ecs.domain.event.Event> binder = new Binder<>(com.lohika.morning.ecs.domain.event.Event.class);

    @Autowired
    public EventEditor(EventRepository repository) {
        super("Edit Event");
        this.repository = repository;

        setModal(true);
        setClosable(false);
        setResizable(false);
        setDraggable(false);
        center();

        VerticalLayout container = new VerticalLayout();
        setContent(container);

        FormLayout fl = new FormLayout(name, description, date, actions);
        container.addComponent(fl);

        // bind using naming convention
        binder.bindInstanceFields(this);

        // Configure and style components
        container.setSpacing(true);

        save.setStyleName(ValoTheme.BUTTON_PRIMARY);
        save.setClickShortcut(ShortcutAction.KeyCode.ENTER);

        // wire action buttons to save, delete and reset
        cancel.addClickListener(clickEvent -> this.hide());
        setVisible(false);
    }

    public final void editEvent(Optional<com.lohika.morning.ecs.domain.event.Event> c) {
        if (c== null  || !c.isPresent()) {
            setVisible(false);
            return;
        }

        com.lohika.morning.ecs.domain.event.Event event = c.get();

        final boolean persisted = event.getId() != null;
        if (persisted) {
            // Find fresh entity for editing
            this.event = repository.findOne(event.getId());
        } else {
            this.event = event;
        }

        save.addClickListener(clickEvent -> {
            repository.save(this.event);
            this.hide();
        });

        delete.addClickListener(clickEvent -> {
            repository.delete(this.event);
            this.hide();
        });

        // Bind event properties to similarly named fields
        // Could also use annotation or "manual binding" or programmatically
        // moving values from fields to entities before saving
        binder.setBean(this.event);

        setVisible(true);

        // A hack to ensure the whole form is visible
        cancel.focus();
        // Select all text in name field automatically
        name.selectAll();
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
}