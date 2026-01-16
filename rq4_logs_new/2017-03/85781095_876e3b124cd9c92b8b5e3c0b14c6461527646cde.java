package tripPlanner.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.ViewResolver;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;
import org.springframework.web.servlet.view.InternalResourceViewResolver;

import tripPlanner.interfaces.CityVisitingInterface;
import tripPlanner.services.citydecider.VisitingCities;
import tripPlanner.services.googlemaps.RoundTripCreator;

@Configuration
@EnableWebMvc
@ComponentScan({ "tripPlanner.*" })
public class BeanConfig extends WebMvcConfigurerAdapter {

	@Bean
	RoundTripCreator getRoundTripCreator()
	{
		return new RoundTripCreator();
	}
	
	
	@Bean
	CityVisitingInterface getVisitingCities()
	{
		return new VisitingCities();
	}
	
    @Bean
    GoogleAPIKey getGoogleAPIKey()
    {
    	return new GoogleAPIKey(System.getenv("GOOGLE_API_KEY"));
    }
    
    @Bean
    public ViewResolver getViewResolver(){
          InternalResourceViewResolver resolver = new InternalResourceViewResolver();
          resolver.setPrefix("/WEB-INF/views/");
          resolver.setSuffix(".jsp");
          return resolver;
    }
}