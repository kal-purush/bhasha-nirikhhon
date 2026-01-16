package petra.ATTSWproject.view.swing;

import org.assertj.swing.fixture.FrameFixture;
import org.assertj.swing.junit.testcase.AssertJSwingJUnitTestCase;
import org.bson.Document;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.MongoDBContainer;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.model.Filters;

import static org.assertj.core.api.Assertions.*;
import static org.assertj.swing.launcher.ApplicationLauncher.*;

import java.util.regex.Pattern;

import javax.swing.JFrame;

import org.assertj.swing.annotation.GUITest;
import org.assertj.swing.core.GenericTypeMatcher;
import org.assertj.swing.core.matcher.JButtonMatcher;
import org.assertj.swing.finder.WindowFinder;

public class StudyRoomE2E extends AssertJSwingJUnitTestCase{

	@ClassRule
	public static final MongoDBContainer mongo = new MongoDBContainer("mongo:4.4.3");

	private static final String DB_NAME = "test-db";
	private static final String COLLECTION_NAME = "test-collection";
	
	private static final String USER1_ID = "1";
	private static final String USER1_NAME = "User1";
	private static final String USER2_ID = "2";
	private static final String USER2_NAME = "User2";
	
	private MongoClient client;
	
	private FrameFixture window;
	
	@Override
	protected void onSetUp() throws Exception {
		String containerIpAddress = mongo.getContainerIpAddress(); 
		Integer mappedPort = mongo.getFirstMappedPort();
		client = new MongoClient(new ServerAddress(containerIpAddress,mappedPort));
		client.getDatabase(DB_NAME).drop();
		addTestUserToDatabase(USER1_ID,USER1_NAME);
		addTestUserToDatabase(USER2_ID,USER2_NAME);
		application("petra.ATTSWproject.app.StudyRoomApp")
			.withArgs("--mongo-host=" + containerIpAddress,
					"--mongo-port=" + mappedPort.toString(),
					"--db-name=" + DB_NAME,
					"--db-collection=" + COLLECTION_NAME)
			.start();
		window = WindowFinder.findFrame(new GenericTypeMatcher<JFrame>(JFrame.class) {
			@Override
			protected boolean isMatching(JFrame frame) {
				return "Study Room View".equals(frame.getTitle()) && frame.isShowing();
			}
		}).using(robot());
	}

	@Override
	protected void onTearDown() {
		client.close();
	}

	@Test @GUITest
	public void testOnStartAllDatabaseElementsAreShown() {
		assertThat(window.list().contents())
			.anySatisfy(e -> assertThat(e).contains(USER1_ID, USER1_NAME))
			.anySatisfy(e -> assertThat(e).contains(USER2_ID, USER2_NAME));
	}

	
	@Test @GUITest
	public void testAddButtonSuccess() {
		window.textBox("idTextBox").enterText("3");
		window.textBox("nameTextBox").enterText("User3");
		window.button(JButtonMatcher.withText("Add")).click();
		assertThat(window.list().contents()).anySatisfy(e -> assertThat(e).contains("3", "User3"));
	}

	@Test @GUITest
	public void testAddButtonError() {
		window.textBox("idTextBox").enterText(USER1_ID);
		window.textBox("nameTextBox").enterText("new one");
		window.button(JButtonMatcher.withText("Add")).click();
		assertThat(window.label("errorMessageLabel").text()).isEqualTo("User with id " + USER1_ID + " already exists");
	}

	@Test @GUITest
	public void testDeleteButtonSuccess() {
		window.list("usersList").selectItem(Pattern.compile(".*" + USER1_NAME + ".*"));
		window.button(JButtonMatcher.withText("Delete")).click();
		assertThat(window.list().contents()).noneMatch(e -> e.contains(USER1_NAME));
	}

	@Test @GUITest
	public void testDeleteButtonError() {
		window.list("usersList").selectItem(Pattern.compile(".*" + USER1_NAME + ".*"));
		RemoveTestUserFromDatabase(USER1_ID);
		window.button(JButtonMatcher.withText("Delete")).click();
		assertThat(window.label("errorMessageLabel").text()).isEqualTo("User with id " + USER1_ID + " does not exist");
	}
	
	private void addTestUserToDatabase(String id, String name) {
		client.getDatabase(DB_NAME).getCollection(COLLECTION_NAME).insertOne(new Document().append("id", id).append("name", name));
	}
	
	private void RemoveTestUserFromDatabase(String id) {
		client.getDatabase(DB_NAME).getCollection(COLLECTION_NAME).deleteOne(Filters.eq("id",id));
	}

}