package org.spiceboys.Travel.Diary.config;

import com.cloudinary.Cloudinary;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import java.util.HashMap;
import java.util.Map;

@Configuration
public class CloudinaryConfig {

    @Bean
    public Cloudinary cloudinary(){
        Map<String, Object> config = new HashMap<>();
        config.put("cloud_name", "dpro85h5s");
        config.put("api_key", "958782353527984");
        config.put("api_secret", "bx1vVEgfN9xZhw4uvLqzG-Re-FM");
        return new Cloudinary(config);
    };


}