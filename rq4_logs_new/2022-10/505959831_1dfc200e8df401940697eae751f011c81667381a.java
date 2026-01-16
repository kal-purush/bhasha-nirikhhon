package by.salov.config;

import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.crypto.password.NoOpPasswordEncoder;
import org.springframework.security.web.authentication.logout.LogoutHandler;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Authentication in memory by default User from userdetails
 */
@EnableWebSecurity
public class InMemoryUserFromUserDetailsSecurityConfiguration extends WebSecurityConfigurerAdapter {

    @Override
    protected void configure(AuthenticationManagerBuilder auth) throws Exception {
        /*
        * Add  authentication for two users, and mark their roles
         */
     auth.inMemoryAuthentication()
             .withUser(User.builder().username("user").password("user").authorities("ROLE_USER"))
             .withUser(User.builder().username("admin").password("admin").authorities("ROLE_ADMIN"))
                /*
                * Add password encoder
                 */
             .passwordEncoder(NoOpPasswordEncoder.getInstance());
    }
    /*
        * Configure access to our url, and configure authorization (adding roles)
        *   /**         - means any urls
        *   /admin/**   - means urls that start fom /admin
        * If we start enumeration urls with common case like /**,
        * then any enumeration stop on it (because any url, including /admin) correspond -> /** ,
        * which means -> all urls will have access. Just because You need to start with narrowly covering templates.
        * Customization login page
     */

    @Override
    protected void configure(HttpSecurity http) throws Exception {
        http.authorizeHttpRequests()
                .antMatchers("/admin/**").hasRole("ADMIN")
                .antMatchers("/user/**").hasAnyRole("USER", "ADMIN")
                .antMatchers("/**").permitAll()
                .and()
                /*customization login page*/
                .formLogin().permitAll()
                .loginPage("/login")
                .loginProcessingUrl("/perform-login")
                .usernameParameter("user")
                .passwordParameter("pass")
                .defaultSuccessUrl("/")
                .and()
                /*customization logout page*/
                .logout()
                .permitAll()
                .logoutUrl("/logout")
                .logoutSuccessUrl("/login");
/*                .deleteCookies("JSESSIONID");*/
/*                .invalidateHttpSession(true);*/
    }
}