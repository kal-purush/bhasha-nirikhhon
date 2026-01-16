package com.makzk.games.slicktest;

import org.newdawn.slick.AppGameContainer;
import org.newdawn.slick.BasicGame;
import org.newdawn.slick.SlickException;
import org.newdawn.slick.util.Log;

public class Main {

	public static void main(String[] args) {
		int winWidth = 1280; // Ancho ventana
		int winHeight = 720; // Alto ventana
		String title = "Prueba Slick2D"; // Título de ventana
		BasicGame test = new CameraTest(title); // Clase a ejecutar
		
		try {
			AppGameContainer appgc;
			appgc = new AppGameContainer(test);
			appgc.setDisplayMode(winWidth, winHeight, false);
			appgc.start();
		} catch (SlickException e) {
			Log.error(null, e);
		}
	}

}