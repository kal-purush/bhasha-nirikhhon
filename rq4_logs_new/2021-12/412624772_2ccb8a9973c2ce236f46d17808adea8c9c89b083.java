package io.bssw.psip.ui.views;

import com.vaadin.flow.component.Component;
import com.vaadin.flow.component.textfield.TextField;
import com.vaadin.flow.component.html.Label;
import com.vaadin.flow.component.orderedlayout.FlexLayout;
import com.vaadin.flow.component.orderedlayout.VerticalLayout;
import com.vaadin.flow.router.*;
import io.bssw.psip.backend.service.ActivityService;
import io.bssw.psip.ui.MainLayout;
import io.bssw.psip.ui.components.FlexBoxLayout;
import io.bssw.psip.ui.layout.size.Horizontal;
import io.bssw.psip.ui.layout.size.Uniform;
import org.springframework.beans.factory.annotation.Autowired;

@PageTitle("Save")
@Route(value = "save", layout = MainLayout.class)
public class Save extends ViewFrame implements HasUrlParameter<String> {
    private ActivityService activityService;
    private VerticalLayout mainLayout;

    public Save(@Autowired ActivityService activityService) {
        this.activityService = activityService;
        setViewContent(createContent());
    }

    private Component createContent() {
        mainLayout = new VerticalLayout();
        mainLayout.setHeightFull();
        FlexBoxLayout content = new FlexBoxLayout(mainLayout);
        content.setFlexDirection(FlexLayout.FlexDirection.COLUMN);
        content.setMargin(Horizontal.AUTO);
        content.setMaxWidth("840px");
        content.setPadding(Uniform.RESPONSIVE_L);
        content.setHeightFull();
        return content;
    }

    @Override
    public void setParameter(BeforeEvent event, @WildcardParameter String parameter){
        if (parameter.isEmpty()) {
            mainLayout.removeAll();
            // Get a file name
            Label label = new Label("Enter Markdown File name"); // We might actually want to just produce the file without bringing them to a save page.
            // Create the file
            // Make it available for download

            //mainLayout.addAndExpand(vrt);
        }
    }
}