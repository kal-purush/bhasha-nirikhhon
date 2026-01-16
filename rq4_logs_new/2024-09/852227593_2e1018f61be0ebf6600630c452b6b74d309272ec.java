package com.example.demo;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.example.demo.modelo.Usuario;
import com.example.demo.service.UsuarioService;

@SpringBootApplication
public class ColeccionistasApplication implements CommandLineRunner {

	@Autowired
	UsuarioService usuarioService;

	public static void main(String[] args) {
		SpringApplication.run(ColeccionistasApplication.class, args);
	}

	@Override
	public void run(String... args) throws Exception {
//		System.out.println("Hola");
		mostrarUsuarios();

	}

	public void mostrarUsuarios() {
		System.out.println("---------------- USUARIOS ----------------");
		List<Usuario> usuarios = usuarioService.usuarios();
		for (Usuario usuario : usuarios) {
			System.out.println(usuario);
		}
	}

}