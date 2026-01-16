package pe.edu.ceid.simi.operative;

import javax.sql.DataSource;
import org.json.simple.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.validation.beanvalidation.MethodValidationPostProcessor;
import org.springframework.web.servlet.config.annotation.ResourceHandlerRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurerAdapter;
import org.springframework.web.servlet.view.InternalResourceViewResolver;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.io.support.EncodedResource;
import org.springframework.core.io.support.PropertySourceFactory;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.core.env.MapPropertySource;
import com.fasterxml.jackson.databind.ObjectMapper;

import pe.edu.ceid.simi.operative.security.Config;
import pe.edu.ceid.simi.operative.security.DatabaseSource;

import org.springframework.core.env.Environment;

@Configuration
@PropertySource(value = Config.CONFIGURATION, factory = AppConfig.JsonLoader.class)
public class AppConfig extends WebMvcConfigurerAdapter {

	@Autowired
	 private Environment env;
	
	@Bean
	public LinkedHashMap<Object, Object> getProperty() {
		JSONObject json = env.getProperty(Config.CONFIG, JSONObject.class);
		@SuppressWarnings("unchecked") LinkedHashMap<Object, Object> database = (LinkedHashMap<Object, Object>) json.get(Config.DATABASE);
		return database;
	}
	
	@Bean
    public MethodValidationPostProcessor methodValidationPostProcessor() {
        return new MethodValidationPostProcessor();
    }

    @Bean
    public InternalResourceViewResolver viewResolver() {
        return new InternalResourceViewResolver();
    }
    
	@Override
	public void addResourceHandlers(ResourceHandlerRegistry registry) {
		registry.addResourceHandler("/**").addResourceLocations("classpath:/static/");
	}
	
	@Bean
    public DataSource getDataSource() {
		DatabaseSource dataSource = DatabaseSource.setProperties(getProperty());
		DataSourceBuilder dataSourceBuilder = DataSourceBuilder.create();
        dataSourceBuilder.driverClassName(dataSource.getDriver()).url(dataSource.getUrl()).username(dataSource.getUsername()).password(dataSource.getPassword());
        return dataSourceBuilder.build();
    }
	
	@Bean("jdbcTemplate")
    JdbcTemplate JdbcTemplate(DataSource centralDataSource) {
        return new JdbcTemplate(centralDataSource);
    }
	
	public static class JsonLoader implements PropertySourceFactory {

		@Override
		@SuppressWarnings("unchecked")
		public org.springframework.core.env.PropertySource<?> createPropertySource(String name,
				EncodedResource resource) {
			
			Map<String, Object> readValue = null;
			InputStream inputStream;
			try {
				inputStream = resource.getInputStream();
				ObjectMapper objectMapper = new ObjectMapper();
				readValue = objectMapper.readValue(inputStream, Map.class);
	            return new MapPropertySource("json-source", readValue);
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
			
		}

    }
}