package com.quick.location.firebase.config;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.springframework.stereotype.Component;

import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.auth.FirebaseCredentials;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;

@Component
public class FirebaseServerApp {

	@PostConstruct
	public static void initializeFirebaseApp() throws IOException {
		FirebaseOptions options = new FirebaseOptions.Builder()
		        .setCredential(FirebaseCredentials.fromCertificate(
		        		FirebaseServerApp.class.getResourceAsStream("/prueba-10d8bAdminSDK.json")))
		        .setDatabaseUrl("https://prueba-10d8b.firebaseio.com").build();
		FirebaseApp.initializeApp(options);
	}

	public  DatabaseReference getDatabaseReference(String url) {
	
		return FirebaseDatabase.getInstance().getReference(url);
	}
	
	public  void newChild(String url,String urlChild,String id,Object objectData)
	{
		DatabaseReference ref = getDatabaseReference(url);
		DatabaseReference dataRef = ref.child(urlChild);
		Map<String, Object> data = new HashMap<>();
		data.put(id, objectData);
		
		dataRef.push().setValue(data);
	}
	
	
	public  void newChild(String url,String urlChild,Map<String, Object> data)
	{
		DatabaseReference ref = getDatabaseReference(url);
		DatabaseReference dataRef = ref.child(urlChild);
		
		dataRef.setValue(data);
	}
}