package com.tmax.ck.main;

import java.io.IOException;

import com.tmax.ck.stress.app.App;

import fi.iki.elonen.NanoHTTPD;

public class Main {
	public static void main(String args[]) {
		System.out.println("Start stress");
		
		App app = new App(8080);
		try {
			app.start(NanoHTTPD.SOCKET_READ_TIMEOUT, false);
			System.out.println("Start server : listen port = 8080");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}