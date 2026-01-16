import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

        import java.io.IOException;
        import java.net.URI;
        import java.net.http.HttpClient;
        import java.net.http.HttpRequest;
        import java.net.http.HttpResponse;

        import static org.junit.jupiter.api.Assertions.*;

public class ProjectsTest {

    //private final HttpClient client = HttpClient.newHttpClient();

    private HttpClient client;
    private ObjectMapper objectMapper;
    @BeforeEach
    public void setup() {
        client = HttpClient.newHttpClient();
        objectMapper = new ObjectMapper();
    }

    @Test
    public void testGetAllProjects() throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:4567/projects"))
                .GET().build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        System.out.println(response.body());
    }

    @Test
    public void testHeadAllProjects() throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:4567/projects"))
                .method("HEAD", HttpRequest.BodyPublishers.noBody())
                .build();

        HttpResponse<Void> response = client.send(request, HttpResponse.BodyHandlers.discarding());
        assertEquals(200, response.statusCode());
    }

    @Test
    public void testGetProjectById() throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:4567/projects/1"))
                .GET().build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        System.out.println(response.body());
    }

    @Test
    public void testCreateProjectWithInvalidCompletedAndActiveStatus() throws IOException, InterruptedException {
        String requestBody = "{ \"title\": \"s aute irure dolor i\", \"completed\": \"true\", \"active\": \"false\", \"description\": \"sse cillum dolore eu\" }";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:4567/projects"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        // Check for expected error response
        assertEquals(400, response.statusCode());
        System.out.println(response.body());
    }

    @Test
    public void testCreateProject() throws IOException, InterruptedException {
        String requestBody = "{ \"title\": \"s aute irure dolor i\", \"active\": false, \"completed\": false, \"description\": \"sse cillum dolore eu\" }";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:4567/projects"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(201, response.statusCode());
        System.out.println(response.body());
    }

    @Test
    public void testGetNonExistentProject() throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:4567/projects/8"))
                .GET().build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(404, response.statusCode());
        System.out.println(response.body());
    }

    @Test
    public void testHeadProjectById() throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:4567/projects/1"))
                .method("HEAD", HttpRequest.BodyPublishers.noBody())
                .build();

        HttpResponse<Void> response = client.send(request, HttpResponse.BodyHandlers.discarding());
        assertEquals(200, response.statusCode());
    }

    @Test
    public void testCreateProjectWithIncorrectId() throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:4567/projects/7"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("")) // Empty body
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(404, response.statusCode());
        System.out.println(response.body());
    }
    @Test
    public void testUpdateProject() throws IOException, InterruptedException {
        String requestBody = "{ \"title\": \" new title \", \"active\": false, \"completed\": false, \"description\": \"new description\"}";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:4567/projects/2"))
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(requestBody)) // Empty body
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        System.out.println(response.body());
    }

    @Test
    public void testUpdateProjectCompletedOnly() throws IOException, InterruptedException {
        String requestBody = "{ \"Completed\": true }";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:4567/projects/7"))
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(404, response.statusCode());
        System.out.println(response.body());
    }

    @Test
    public void testUpdateProjectWithAllFields() throws IOException, InterruptedException {
        String requestBody = "{ \"title\": \"Updated Title\", \"active\": true, \"completed\": false, \"description\": \"Updated Description\" }";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:4567/projects/1"))
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        System.out.println(response.body());
    }

    @Test
    public void testDeleteProject() throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:4567/projects/1"))
                .DELETE()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        System.out.println(response.body());
    }

    @Test
    public void testUpdateProjectTitle() throws IOException, InterruptedException {
        String requestBody = "{ \"title\": \"New title\" }";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:4567/projects/1"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        System.out.println(response.body());
    }

    @Test
    public void testUpdateNonExistentProject() throws IOException, InterruptedException {
        String requestBody = "{ \"title\": \"Title\" }";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:4567/projects/32"))
                .header("Content-Type", "application/json")
                .PUT(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(404, response.statusCode());
        System.out.println(response.body());
    }

    @Test
    public void testGetCategoriesForProject() throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:4567/projects/1/categories"))
                .GET().build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        System.out.println(response.body());
    }

    @Test
    public void testHeadCategoriesForProject() throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:4567/projects/1/categories"))
                .method("HEAD", HttpRequest.BodyPublishers.noBody())
                .build();

        HttpResponse<Void> response = client.send(request, HttpResponse.BodyHandlers.discarding());
        assertEquals(200, response.statusCode());
    }

    @Test
    public void testCreateLinkBetweenProjectAndCategory() throws IOException, InterruptedException {
        String requestBody = "{ \"id\": \"1\" }";  // Assuming category 1 exists

        HttpRequest createLinkRequest = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:4567/projects/1/categories"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        HttpResponse<String> createLinkResponse = client.send(createLinkRequest, HttpResponse.BodyHandlers.ofString());
        assertEquals(201, createLinkResponse.statusCode(), "Failed to create link between project and category");

        HttpRequest deleteRequest = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:4567/projects/1/categories/1"))
                .DELETE()
                .build();

        HttpResponse<String> deleteResponse = client.send(deleteRequest, HttpResponse.BodyHandlers.ofString());

        assertEquals(200, deleteResponse.statusCode(), "Failed to delete link between project and category");

        System.out.println(deleteResponse.body());
    }

    @Test
    public void testCreateLinkBetweenProjectAndInvalidCategoryId() throws IOException, InterruptedException {
        String requestBody = "{ \"Id\": \"999\" }";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:4567/projects/1/categories"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(404, response.statusCode());
        System.out.println(response.body());
    }

    @Test
    public void testDeleteLinkBetweenProjectAndCategory() throws IOException, InterruptedException {
        // Step 1: Ensure the project exists
        String projectBody = "{ \"title\": \"Test Project\", \"description\": \"A sample project\" }";

        HttpRequest createProjectRequest = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:4567/projects"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(projectBody))
                .build();

        HttpResponse<String> createProjectResponse = client.send(createProjectRequest, HttpResponse.BodyHandlers.ofString());
        assertTrue(createProjectResponse.statusCode() == 201 || createProjectResponse.statusCode() == 200,
                "Failed to create or confirm project");

        // Step 2: Ensure the category exists
        String categoryBody = "{ \"name\": \"Test Category\" }";

        HttpRequest createCategoryRequest = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:4567/categories"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(categoryBody))
                .build();

        HttpResponse<String> createCategoryResponse = client.send(createCategoryRequest, HttpResponse.BodyHandlers.ofString());
        assertTrue(createCategoryResponse.statusCode() == 201 || createCategoryResponse.statusCode() == 200,
                "Failed to create or confirm category");

        // Step 3: Create the link between the project and category
        String requestBody = "{ \"id\": \"1\" }";  // Assuming we want to link category 1

        HttpRequest createLinkRequest = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:4567/projects/1/categories"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        HttpResponse<String> createLinkResponse = client.send(createLinkRequest, HttpResponse.BodyHandlers.ofString());
        assertEquals(201, createLinkResponse.statusCode(), "Failed to create link between project and category");

        // Step 4: Now, delete the link between the project and the category
        HttpRequest deleteRequest = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:4567/projects/1/categories/1"))
                .DELETE()
                .build();

        HttpResponse<String> deleteResponse = client.send(deleteRequest, HttpResponse.BodyHandlers.ofString());

        // Verify that the deletion was successful
        assertEquals(200, deleteResponse.statusCode(), "Failed to delete link between project and category");

        // Optionally, print the response body for debugging purposes
        System.out.println(deleteResponse.body());
    }


    @Test
    public void testDeleteNonExistentLinkBetweenProjectAndCategory() throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:4567/projects/1/categories/999"))
                .DELETE()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(404, response.statusCode());
        System.out.println(response.body());
    }

    @Test
    public void testGetTasksProject() throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:4567/projects/1/tasks"))
                .GET().build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        System.out.println(response.body());
    }

    @Test
    public void testHeadTasksProject() throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:4567/projects/1/tasks"))
                .method("HEAD", HttpRequest.BodyPublishers.noBody())
                .build();

        HttpResponse<Void> response = client.send(request, HttpResponse.BodyHandlers.discarding());
        assertEquals(200, response.statusCode());
    }

    @Test
    public void testCreateNewTasksLinkWithProject() throws IOException, InterruptedException {
        String requestBody = "{ \"Id\": \"1\" }";
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:4567/projects/1/tasks"))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(201, response.statusCode());
        System.out.println(response.body());
    }

    @Test
    public void testDeleteTaskLinkWithProject() throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:4567/projects/1/tasks/1"))
                .DELETE()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        System.out.println(response.body());
    }

    @Test
    public void testDeleteNonExistentProject() throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:4567/projects/999")) // Non-existent ID
                .DELETE()
                .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(404, response.statusCode()); // Expecting 404 for non-existent ID
        System.out.println(response.body());
    }

    @Test
    public void testGetProjectsByCompleted() throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:4567/projects?completed=false"))
                .GET().build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        System.out.println(response.body());
    }

    @Test
    public void testGetProjectsByActive() throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://localhost:4567/projects?active=true"))
                .GET().build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode());
        System.out.println(response.body());
    }
}
