package com.jugovicm.registrationlogin.config;

import com.jugovicm.registrationlogin.service.UserService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.web.servlet.WebMvcAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.authentication.dao.DaoAuthenticationProvider;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityCustomizer;
import org.springframework.security.config.annotation.web.configurers.DefaultLoginPageConfigurer;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;

@Configuration
@EnableWebSecurity
public class SecurityConfiguration extends WebMvcAutoConfiguration {

    @Autowired
    private UserService userService;

    @Bean
    public BCryptPasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder ();
    }
    @Autowired
    public DaoAuthenticationProvider authenticationProvider() {
        DaoAuthenticationProvider authProvider = new DaoAuthenticationProvider ();

        authProvider.setUserDetailsService(userService);
        authProvider.setPasswordEncoder(passwordEncoder());

        return authProvider;
    }

    @Autowired
    public void configureGlobal(AuthenticationManagerBuilder auth) throws Exception {
        auth.authenticationProvider ( authenticationProvider () );
    }

    @Autowired
    public void configure(HttpSecurity http) throws Exception {
        /*http.authorizeRequests().requestMatchers (
                        "/registration**",
                        "/js/**",
                        "/css/**",
                        "/img/**").permitAll()
                .anyRequest().authenticated()
                .and()
                .formLogin()
                .loginPage("/login")
                .permitAll()
                .and()
                .logout()
                .invalidateHttpSession(true)
                .clearAuthentication(true)
                .logoutRequestMatcher(new AntPathRequestMatcher("/logout"))
                .logoutSuccessUrl("/login?logout")
                .permitAll();*/
        http.csrf().disable()
                .authorizeRequests().requestMatchers ( HttpMethod.OPTIONS,"*/").permitAll()
                .requestMatchers (HttpMethod.GET,"/login").permitAll();
        http.removeConfigurer( DefaultLoginPageConfigurer.class);
        http.
                authorizeRequests()
                .requestMatchers ("/resources/**","/login", "/home").permitAll()
                .requestMatchers ("/user/**").hasAuthority ("USER")


                .and()
                .formLogin().loginPage("/login").permitAll()
                .defaultSuccessUrl("/user/dashboard")
                .failureUrl("/login-error")

                .and().logout().permitAll();
    }

    @Bean
    public WebSecurityCustomizer webSecurityCustomizer() {
        return web -> {
            web.ignoring ().requestMatchers ( "/registration**", "/img/**", "/js/**", "/css/**" );
        };
    }
}