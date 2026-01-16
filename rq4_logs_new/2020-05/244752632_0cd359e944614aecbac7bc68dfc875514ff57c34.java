package com.app.colibri.controller;

import static com.app.colibri.service.AppSettings.jsonMapper;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.mindrot.jbcrypt.BCrypt;

import com.app.colibri.model.User;
import com.app.colibri.registry.UserDataRegistry;
import com.app.colibri.service.AppSettings;
import com.app.colibri.service.crypt.CryptoUtils;

public class RESTController {

	public static int login(User user, String password) {
		try {
			final String[] encryptedUserName = { CryptoUtils.encrypt(AppSettings.KEY, user.getUserName()) };

			String responseJson = postJson("loginstart", jsonMapper.writeValueAsString(encryptedUserName));

			String[] responseArray = jsonMapper.readValue(responseJson, String[].class);

			if (responseArray[0].equals("200")) {
				System.out.println("web user login start SUCCESS");

				try {
					String salt = responseArray[1];
					String authTokenBufferEncypted = responseArray[2];
					String hash = BCrypt.hashpw(password, salt);

					final String authToken = CryptoUtils.decrypt(hash.substring(hash.length() - 16, hash.length()),
							authTokenBufferEncypted);

					final String authTokenEncrypted = CryptoUtils.encrypt(authToken, authToken);

					String[] encryptedUserNameToken = { encryptedUserName[0], authTokenEncrypted };

					String responseJsonLC = postJson("loginconfirm", jsonMapper.writeValueAsString(encryptedUserNameToken));

					Integer[] responseArryaLC = jsonMapper.readValue(responseJsonLC, Integer[].class);

					if (responseArryaLC[0] == 200) {
						user.setId(responseArryaLC[1]);
						user.setAuthorizationToken(authToken);
					} else {
						user.setId(0);
						user.setAuthorizationToken(null);
						return 406;
					}

				} catch (Exception e) {
					return 406; // HttpStatus.NOT_ACCEPTABLE
				}

				return 200;
			} else if (responseArray[0].equals("404")) {
				System.out.println("web user NOT FOUND");
				return 404;
			} else {
				System.out.println("web user login FAIL");
				return 400;
			}

		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Web user login FAIL");
			return 400;
		}

	}

	public static int register(User user) {
		try {
			String jsonUser = jsonMapper.writeValueAsString(user);
			String[] encryptedJsonUser = { CryptoUtils.encrypt(AppSettings.KEY, jsonUser) };

			String responseJson = postJson("register", jsonMapper.writeValueAsString(encryptedJsonUser));

			Integer[] responseArray = jsonMapper.readValue(responseJson, Integer[].class);

			if (responseArray[0] == 201) {
				System.out.println("web user registration SUCCESS");
			} else if (responseArray[0] == 409) {
				System.out.println("web user already EXISTS");
			} else {
				System.out.println("web user registration FAIL");
			}

			return responseArray[0];

		} catch (Exception e) {
			e.printStackTrace();
			return 400;
		}
	}

	public static UserDataRegistry getUserData() {
		try {
			User user = AppSettings.appSettings.getUser();

			Integer[] request = { user.getId() };

			String responseJson = postJson("getuserdata", jsonMapper.writeValueAsString(request));

			String[] responseArray = jsonMapper.readValue(responseJson, String[].class);

			UserDataRegistry webUserDataRegistry = null;

			if (responseArray[0].equals("200")) {
				webUserDataRegistry = jsonMapper.readValue(CryptoUtils.decrypt(user.getAuthorizationToken(), responseArray[1]),
						UserDataRegistry.class);
			}

			return webUserDataRegistry;

		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	public static boolean sendUserData() {
		try {
			User user = AppSettings.appSettings.getUser();

			String[] encryptedRequest = { String.valueOf(user.getId()), CryptoUtils.encrypt(user.getAuthorizationToken(),
					jsonMapper.writeValueAsString(WordController.userDataRegistry)) };

			String responseJson = postJson("senduserdata", jsonMapper.writeValueAsString(encryptedRequest));

			Integer[] responseArray = jsonMapper.readValue(responseJson, Integer[].class);

			return (responseArray[0] == 200);

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	public static String postJson(String urlAction, String json) {
		try {
			CloseableHttpClient httpClient = HttpClientBuilder.create().build();
			HttpPost postRequest = new HttpPost(AppSettings.APP_URL + urlAction);

			StringEntity input = new StringEntity(json, ContentType.APPLICATION_JSON);
			input.setContentType("application/json");
			postRequest.setEntity(input);

			return getJsonFromResponce(httpClient.execute(postRequest));
		} catch (Exception e) {
			return null;
		}
	}

	public static String getJsonFromResponce(HttpResponse response) {
		try {
			BufferedReader br = new BufferedReader(new InputStreamReader((response.getEntity().getContent())));

			String responseJson = "";
			String output;

			while ((output = br.readLine()) != null) {
				responseJson += output;
			}

			return responseJson;
		} catch (Exception e) {
			return null;
		}
	}

	public static boolean checkConnection() {
		try {

			CloseableHttpClient httpClient = HttpClientBuilder.create().build();
			HttpGet getRequest = new HttpGet(AppSettings.APP_URL + "checkconnection");

			HttpResponse response = httpClient.execute(getRequest);

			return (response.getStatusLine().getStatusCode() == 200);

		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

}