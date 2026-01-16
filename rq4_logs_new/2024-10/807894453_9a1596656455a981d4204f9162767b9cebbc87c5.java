package com.adminportal.config;  // Package declaration

import com.adminportal.service.impl.UserSecurityService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.dao.DaoAuthenticationProvider;
import org.springframework.security.config.annotation.authentication.configuration.AuthenticationConfiguration;
import org.springframework.security.config.annotation.method.configuration.EnableMethodSecurity;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.web.SecurityFilterChain;


@Configuration  // Indicates that this class is a Spring configuration class
@EnableMethodSecurity(prePostEnabled = true)  // Enables method-level security (e.g., @PreAuthorize, @PostAuthorize)
public class SecurityConfig {
    @Autowired
    private final UserSecurityService userSecurityService;  // Injects UserSecurityService to manage user authentication

    // Constructor-based dependency injection for UserSecurityService
    public SecurityConfig(UserSecurityService userSecurityService) {

        this.userSecurityService = userSecurityService;
    }

    // Defines a BCryptPasswordEncoder bean with the name "configPasswordEncoder" for password encoding
    @Bean(name = "configPasswordEncoder")
    public BCryptPasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder();  // Returns a new BCryptPasswordEncoder instance
    }

    // Defines an array of publicly accessible URLs (no authentication required)
    private static final String[] PUBLIC_MATCHERS = {
            "/css/**",  // Publicly accessible: CSS files
            "/js/**",  // Publicly accessible: JavaScript files
            "/image/**",  // Publicly accessible: Images
            "/",  // Publicly accessible: Root URL
            "/newUser",
            "/forgetPassword",
            "/login",
            "/fonts/**"
    };

    // Defines the security filter chain that configures the application's security settings
    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        http
                .authorizeHttpRequests(authorize -> authorize
                        .requestMatchers(PUBLIC_MATCHERS).permitAll()  // Allow public access to the defined matchers
                        .anyRequest().authenticated()  // All other requests require authentication
                )
                .csrf(csrf -> csrf.disable())  // Disables CSRF protection
                .cors(cors -> cors.disable())  // Disables Cross-Origin Resource Sharing (CORS) protection
                .formLogin(form -> form
                        .loginPage("/login").permitAll()  // Custom login page accessible to everyone
                        .defaultSuccessUrl("/")  // Redirect to the root URL after successful login
                        .failureUrl("/login?error")  // Redirect to the login page with an error parameter on failure
                )
                .logout(logout -> logout
                        .logoutUrl("/logout")  // Custom logout URL
                        .logoutSuccessUrl("/?logout")  // Redirect to the root URL after successful logout
                        .deleteCookies("remember-me").permitAll()  // Deletes the "remember-me" cookie on logout and allows everyone to access logout
                )
                .rememberMe(rememberMe -> rememberMe
                        .tokenValiditySeconds(86400)  // Remember-me token is valid for 86400 seconds (1 day)
                        .key("mySecretKey")  // Sets a key for token hashing (should be a strong secret)
                        .userDetailsService(userSecurityService)  // Tells remember-me functionality to use the custom UserDetailsService
                );

        return http.build();  // Builds the HttpSecurity object and returns it as a SecurityFilterChain
    }

    // Defines the AuthenticationManager bean, which is used for handling authentication
    @Bean
    public AuthenticationManager authenticationManager(AuthenticationConfiguration authenticationConfiguration) throws Exception {
        return authenticationConfiguration.getAuthenticationManager();
    }

    @Bean
    public DaoAuthenticationProvider daoAuthenticationProvider(BCryptPasswordEncoder passwordEncoder, UserDetailsService userDetailsService) {
        DaoAuthenticationProvider provider = new DaoAuthenticationProvider();
        provider.setUserDetailsService(userDetailsService);
        provider.setPasswordEncoder(passwordEncoder);
        return provider;
    }

    // Defines the UserDetailsService bean, which Spring Security uses to load user data for authentication
    @Bean
    public UserDetailsService userDetailsService() {
        return userSecurityService;  // Returns the injected custom UserSecurityService, which implements UserDetailsService
    }
}