package com.web.controller;

import com.web.domain.CareerTestQuestion;
import com.web.domain.CareerTestAnswer;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

@RestController
public class CareerPsychologicalTestController {

	private static final String API_KEY = "92a66e624117c92a3fd4ee8379ee5a00"; // 본인의 API 키로 교체
	private static final String TEST_ID = "6"; // 식별자가 고정된 경우

	@GetMapping("/career-test")
	public List<CareerTestQuestion> getCareerTestQuestions() {
		List<CareerTestQuestion> questions = new ArrayList<>();
		try {
			String apiUrl = "http://www.career.go.kr/inspct/openapi/test/questions";
			String queryString = String.format("?apikey=%s&q=%s", URLEncoder.encode(API_KEY, "UTF-8"), TEST_ID);

			URL url = new URL(apiUrl + queryString);
			HttpURLConnection conn = (HttpURLConnection) url.openConnection();

			try (BufferedReader rd = new BufferedReader(new InputStreamReader(conn.getInputStream()))) {
				StringBuilder response = new StringBuilder();
				String line;
				while ((line = rd.readLine()) != null) {

					response.append(line);

				}
				questions = parseCareerTestQuestions(response.toString());
			} finally {
				conn.disconnect();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return questions;
	}

	private List<CareerTestQuestion> parseCareerTestQuestions(String responseData) {
		List<CareerTestQuestion> questions = new ArrayList<>();
		JSONParser parser = new JSONParser();
		System.out.println(responseData);
		try {
			JSONObject responseJson = (JSONObject) parser.parse(responseData);
			System.out.println(responseJson);
		} catch (Exception e) {
			
		}

		try {
			JSONObject responseJson = (JSONObject) parser.parse(responseData);
			JSONArray resultArray = (JSONArray) responseJson.get("RESULT");

			for (Object obj : resultArray) {
				JSONObject questionJson = (JSONObject) obj;
				CareerTestQuestion question = new CareerTestQuestion();
				question.setQuestion((String) questionJson.get("question"));
				
				
				List<CareerTestAnswer> answers = new ArrayList<>();
				for (int i = 1; i <= 4; i++) {
					String answerKey = "answer" + String.format("%02d", i);
					if (questionJson.containsKey(answerKey)) {
						answers.add(new CareerTestAnswer((String) questionJson.get(answerKey), i));
						System.out.println(questionJson.get(answerKey));
					} else {
						break;
					}
				}

				question.setAnswers(answers);
				questions.add(question);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		return questions;
	}
}