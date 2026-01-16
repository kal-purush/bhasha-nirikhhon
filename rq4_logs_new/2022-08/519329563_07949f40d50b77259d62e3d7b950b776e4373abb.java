package br.com.alura.mvc.mudi;

import javax.sql.DataSource;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.rest.webmvc.config.RepositoryRestConfigurer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder;
import org.springframework.security.provisioning.JdbcUserDetailsManager;
import org.springframework.security.provisioning.UserDetailsManager;
import org.springframework.security.web.SecurityFilterChain;

import br.com.alura.mvc.mudi.model.Pedido;

@Configuration
@EnableWebSecurity
public class SecurityConfiguration {

	@Autowired
	private DataSource datasource;

	@Bean
	public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
		http.authorizeHttpRequests(
				(authz) -> authz
					.antMatchers("/home/**").permitAll()
					.anyRequest()
					.authenticated()
					)
			.formLogin(form -> form.loginPage("/login")
					.defaultSuccessUrl("/usuario/pedidos", true).permitAll())
			.logout(logout -> logout.logoutUrl("/logout")
					.logoutSuccessUrl("/home"))
			.cors().and().csrf().disable();

		return http.build();
	}

    @Bean
    public UserDetailsManager users() {
    	BCryptPasswordEncoder encoder = new BCryptPasswordEncoder();
//		UserDetails user = User.builder().username("maria").password(encoder.encode("123456")).roles("ADM").build();
		
        JdbcUserDetailsManager users = new JdbcUserDetailsManager(datasource);
//        users.createUser(user);
        return users;
    }



}