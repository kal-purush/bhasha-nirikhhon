package by.ras.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.UserDetailsService;
import org.springframework.security.web.util.matcher.AntPathRequestMatcher;

@Configuration
@EnableWebSecurity
public class SecurityConfig extends WebSecurityConfigurerAdapter {

    private final UserDetailsService userDetailsService;

    @Autowired
    public SecurityConfig(UserDetailsService userDetailsService) {
        this.userDetailsService = userDetailsService;
    }

    @Override
    protected void configure(HttpSecurity http) throws Exception {
        http
                .authorizeRequests()
//                .antMatchers("/market/*").authenticated()
//                .antMatchers("/market/admin/*").hasAuthority("ADMIN")
//                .antMatchers("/user", "/usertest", "/market", "/market/user/*", "/market/user/usertest").hasAuthority("CLIENT")
                .anyRequest().authenticated()
                .and()
                .formLogin().loginPage("/").permitAll()
                .and()
                .logout().logoutRequestMatcher(new AntPathRequestMatcher("/logout")).permitAll()
//                .and()
//                .userDetailsService(userDetailsService)
        ;
        //http.csrf().disable();

        //get credetials of current user session
        //SecurityContextHolder.getContext().getAuthentication().getName();
    }

}