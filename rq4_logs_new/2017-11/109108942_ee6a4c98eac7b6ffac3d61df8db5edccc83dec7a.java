package org.bana.contextpath;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.support.SpringBootServletInitializer;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

@SpringBootApplication
public class ContextPathApplication extends SpringBootServletInitializer {

	public static void main(String[] args) {
		SpringApplication.run(ContextPathApplication.class, args);
	}
	
	
	@EnableWebSecurity
	public class Keamanan extends WebSecurityConfigurerAdapter {

		@Autowired
		protected void configure(AuthenticationManagerBuilder auth) throws Exception {
			// TODO Auto-generated method stub
			auth
				.inMemoryAuthentication()
					.withUser("bana").password("123").roles("ADMIN");
		}

		@Override
		protected void configure(HttpSecurity http) throws Exception {
			// TODO Auto-generated method stub
			http
				.authorizeRequests()
				.anyRequest().authenticated()
				.and()
				.formLogin().loginPage("/login").defaultSuccessUrl("/tes1", true).permitAll()
				.and()
				.csrf().disable();
		}
		
	}
	
	@Controller
	public class Tes {
		
		@GetMapping("/tes1")
		public String tes1() {
			return "index1";
		}
		
		@GetMapping("/tes2")
		public String tes2() {
			return "index2";
		}

		@GetMapping("/login")
		public String showLogin() {
			return "login";
		}
		
		@GetMapping("")
		public String toShowLogin() {
			return "redirect:/login";
		}
		
	}

	@Override
	protected SpringApplicationBuilder configure(SpringApplicationBuilder builder) {
		// TODO Auto-generated method stub
		return builder.sources(ContextPathApplication.class);
	}
}