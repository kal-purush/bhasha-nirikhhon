package ua.compservice.test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import com.fasterxml.jackson.annotation.JsonAlias;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@DisplayName("Annotations tests")
class AnnotationTests {

	@DisplayName("Demo of using @JsonAlias annotation...")
	@Test
	void testAlias() throws JsonParseException, JsonMappingException, IOException {

		String json = "{\n" + "  \"n\": \"Dimkas\",\n" + "  \"age\": 46\n" + "}";

		ObjectMapper mapper = new ObjectMapper();

		Pojo aPojo = mapper.readValue(json, Pojo.class);

		assertThat(aPojo.getName(), is(equalTo("Dimkas")));
		assertThat(aPojo.getAge(), is(equalTo(46)));

		String anotherJson = "{\n" + "  \"someName\": \"Dimkas\",\n" + "  \"someAge\": 46\n" + "}";

		mapper = new ObjectMapper();

		aPojo = mapper.readValue(anotherJson, Pojo.class);

		assertThat(aPojo.getName(), is(equalTo("Dimkas")));
		assertThat(aPojo.getAge(), is(equalTo(46)));

	}

}

class Pojo {

	@JsonAlias(value = { "n", "someName" })
	private String name;

	@JsonAlias(value = { "someAge" })
	private int age;

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

}