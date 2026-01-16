package Spring_bean_scope.com.Joker.Dome;

import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Scope;

/**
 * @author Joker
 * @Date 2022/11/5 20:44
 * @Description This is description of class
 * @since version-1.0
 */
public class AppConfig {

    @Bean
    @Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
    public Notepad3 notepad3(){
        return new Notepad3();
    }
}