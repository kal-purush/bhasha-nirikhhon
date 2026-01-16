package runner;

import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.assistedinject.FactoryModuleBuilder;
import com.google.inject.name.Names;
import runner.pages.BasePage;
import runner.pages.CoursePage;
import runner.pages.PageFactory;

public class BinderModule implements Module {

    @Override
    public void configure(Binder binder) {

        binder.install(new FactoryModuleBuilder()
                .implement(BasePage.class, Names.named("mainPage"), MainPage.class)
                .implement(BasePage.class, Names.named("coursePage"), CoursePage.class)
                .build(PageFactory.class));
    }
}
