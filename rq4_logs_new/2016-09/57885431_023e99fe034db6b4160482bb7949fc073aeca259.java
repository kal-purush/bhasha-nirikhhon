package org.vaadin.guice.security;

import com.vaadin.guice.annotation.GuiceView;
import com.vaadin.navigator.View;
import com.vaadin.navigator.ViewChangeListener;
import com.vaadin.ui.Label;

@GuiceView(name = "permissionDeniedGuiceDefault")
public class PermissionDeniedDefaultView extends Label implements View {

    PermissionDeniedDefaultView(){
        super("permission denied");
    }

    public void enter(ViewChangeListener.ViewChangeEvent viewChangeEvent) {
    }
}