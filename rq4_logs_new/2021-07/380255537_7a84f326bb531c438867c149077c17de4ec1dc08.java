package petra.ATTSWproject.controller;

import static java.util.Arrays.asList;
import static org.mockito.Mockito.verify;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testcontainers.containers.MongoDBContainer;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;

import petra.ATTSWproject.model.User;
import petra.ATTSWproject.repository.StudyRoomMongoRepository;
import petra.ATTSWproject.view.StudyRoomView;

public class StudyRoomControllerIT{
	
	@ClassRule
	public static final MongoDBContainer mongo = new MongoDBContainer("mongo:4.4.3");
	
	@Mock
	private StudyRoomView studyRoomView;
	
	private AutoCloseable closeable;
	private MongoClient client;
	private StudyRoomController studyRoomController;
	private StudyRoomMongoRepository studyRoomMongoRepository;
	
	@Before
	public void setUp() {
		closeable = MockitoAnnotations.openMocks(this);
		client = new MongoClient(new ServerAddress(mongo.getContainerIpAddress(),mongo.getFirstMappedPort()));
		studyRoomMongoRepository = new StudyRoomMongoRepository(client);
		for (User student : studyRoomMongoRepository.findAll()) {
			studyRoomMongoRepository.delete(student.getId());
		}
		studyRoomController = new StudyRoomController(studyRoomView, studyRoomMongoRepository, 10);
	}
	
	@After
	public void releaseMocks() throws Exception {
		closeable.close();
	}
	
	@Test
	public void testAllStudents() {
		User user = new User("1", "test");
		studyRoomMongoRepository.save(user);
		studyRoomController.allUsers();
		verify(studyRoomView).showAllUsers(asList(user));
	}

	@Test
	public void testNewStudent() {
		User user = new User("1", "test");
		studyRoomController.newUser(user);
		verify(studyRoomView).userAdded(user);
	}

	@Test
	public void testDeleteStudent() {
		User user = new User("1", "test");
		studyRoomMongoRepository.save(user);
		studyRoomController.deleteUser(user);
		verify(studyRoomView).userRemoved(user);
	}


}