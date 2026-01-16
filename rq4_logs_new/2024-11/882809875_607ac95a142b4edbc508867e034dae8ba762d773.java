/**
 * File: CorsConfig.java
 * Description: Configures Cross-Origin Resource Sharing (CORS) for the application to allow specific client access.
 * Author: Your Name
 * Date: YYYY-MM-DD
 * License: MIT License (if applicable)
 */

 package org.wecancodeit.backend.BOService;

 import org.springframework.context.annotation.Configuration;
 import org.springframework.web.servlet.config.annotation.CorsRegistry;
 import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;
 
 /**
  * Configures Cross-Origin Resource Sharing (CORS) for backend services, allowing specified
  * client addresses to access the resources.
  * 
  * <p>This configuration allows only the defined `allowedClientAddress` to make requests, improving 
  * security by restricting CORS to the specified origin.</p>
  */
 @Configuration
 public class CorsConfig implements WebMvcConfigurer {
 
     // Allowed client origin for CORS requests
     private final String allowedClientAddress = "http://localhost:3000";
 
     /**
      * Configures CORS mappings, setting allowed origins for specified endpoints.
      * 
      * <p>This method adds a global CORS mapping that applies to all endpoints, allowing requests
      * from the specified `allowedClientAddress` only.</p>
      *
      * @param registry The CORS registry to configure with allowed origins.
      */
     @Override
     public void addCorsMappings(CorsRegistry registry) {
         // Registering CORS settings for all endpoints with the allowed origin
         registry.addMapping("/**")
                 .allowedOrigins(allowedClientAddress);
     }
 }
 
 