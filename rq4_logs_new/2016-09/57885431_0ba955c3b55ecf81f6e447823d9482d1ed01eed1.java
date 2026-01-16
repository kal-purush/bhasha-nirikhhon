package org.vaadin.guice.security.cdi;

import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;

import com.vaadin.guice.annotation.GuiceViewChangeListener;
import com.vaadin.guice.annotation.UIScope;
import com.vaadin.guice.server.NavigatorProvider;
import com.vaadin.navigator.View;
import com.vaadin.navigator.ViewChangeListener;
import com.vaadin.ui.Component;

import org.vaadin.guice.security.annotations.GuardedBy;
import org.vaadin.guice.security.api.ComponentVisibilityEvaluator;
import org.vaadin.guice.security.api.Guard;
import org.vaadin.guice.security.api.PermissionEvaluator;
import org.vaadin.guice.security.annotations.NeedsPermission;

import java.util.Set;

@GuiceViewChangeListener
@UIScope
class NavigationAccessChecker implements ViewChangeListener, ComponentVisibilityEvaluator{

    @Inject
    private PermissionEvaluator permissionEvaluator;

    @Inject
    @Named("guice_security_permission_denied_view")
    private String permissionDeniedView;

    @Inject
    private NavigatorProvider navigatorProvider;

    @Inject
    @AllGuards
    private Set<Guard> allGuards;

    @Inject
    @AllRestrictedComponents
    private Set<Component> restrictedComponents;

    public boolean beforeViewChange(ViewChangeEvent viewChangeEvent) {

        final Class<? extends View> newViewClass = viewChangeEvent.getNewView().getClass();

        final NeedsPermission needsPermission = newViewClass.getAnnotation(NeedsPermission.class);

        final GuardedBy guardedBy = newViewClass.getAnnotation(GuardedBy.class);

        boolean isPermitted = isPermitted(needsPermission, guardedBy);

        if(!isPermitted){
            navigateToPermissionDeniedViewIfPresent();
        }

        return isPermitted;
    }

    private boolean isPermitted(NeedsPermission needsPermission, GuardedBy guardedBy) {

        if(needsPermission != null){
            if(!permissionEvaluator.hasPermission(needsPermission.permission())){
                return false;
            }
        }

        if(guardedBy != null){
            for (Class<? extends Guard> guardClass : guardedBy.guards()) {
                for (Guard guard : allGuards) {
                    if(guard.getClass().equals(guardClass)){
                        if(!guard.hasAccess()){
                            return false;
                        }
                    }
                }
            }
        }

        return true;
    }

    private void navigateToPermissionDeniedViewIfPresent(){
        if(permissionDeniedView != null){
            navigatorProvider.get().navigateTo(permissionDeniedView);
        }
    }

    public void afterViewChange(ViewChangeEvent viewChangeEvent) {
    }

    public void evaluate() {
        for (Component restrictedComponent : restrictedComponents) {
            Class<? extends Component> restrictedComponentClass = restrictedComponent.getClass();

            final NeedsPermission needsPermission = restrictedComponentClass.getAnnotation(NeedsPermission.class);

            final GuardedBy guardedBy = restrictedComponentClass.getAnnotation(GuardedBy.class);

            restrictedComponent.setVisible(isPermitted(needsPermission, guardedBy));
        }
    }
}