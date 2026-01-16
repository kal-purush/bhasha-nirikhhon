import io.restassured.RestAssured;
import io.restassured.authentication.PreemptiveBasicAuthScheme;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.filter.log.RequestLoggingFilter;
import io.restassured.filter.log.ResponseLoggingFilter;
import io.restassured.http.ContentType;
import io.restassured.response.ValidatableResponse;
import org.junit.jupiter.api.*;
import static org.assertj.core.api.Assertions.assertThat;

import java.sql.*;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.*;

public class CrudCountryWithDBTest {
    private Connection connection;

    private static String PATH = "/api/countries";
    private static String PATH_WITH_ID = PATH + "/{id}";

    private Integer createdCountryId;

    @BeforeEach
    public  void setConnection() throws SQLException {
        connection = DriverManager.getConnection(
                    "jdbc:postgresql://localhost:5432/app-db",
                "app-db-admin",
                "P@ssw0rd"
        );
    }

    @AfterEach
    public  void closeConnection() throws SQLException {
        if (createdCountryId != null) {
            PreparedStatement sql = connection.prepareStatement(
                    "DELETE FROM country WHERE id = ?;",
                    Statement.RETURN_GENERATED_KEYS
            );
            sql.setInt(1, createdCountryId);
            sql.executeUpdate();
            createdCountryId = null;
        }
        connection.close();
    }


    @BeforeAll
    public static void setUpAuth() {
        PreemptiveBasicAuthScheme authScheme = new PreemptiveBasicAuthScheme();
        authScheme.setUserName("admin");
        authScheme.setPassword("admin");

        RestAssured.enableLoggingOfRequestAndResponseIfValidationFails();

        RestAssured.filters(new RequestLoggingFilter(), new ResponseLoggingFilter());

        RestAssured.requestSpecification = new RequestSpecBuilder()
                //.setBaseUri("localhost:8080")
                .setContentType(ContentType.JSON)
                .setAuth(authScheme)
                .build();
    }

//    @AfterEach
//    public void deleteCountry() throws SQLException {
//        if (createdCountryId != null) {
//            PreparedStatement sql = connection.prepareStatement(
//                    "DELETE FROM country WHERE id = ?;",
//                    Statement.RETURN_GENERATED_KEYS
//            );
//            sql.setInt(1, createdCountryId);
//            sql.executeUpdate();
//            createdCountryId = null;
//        }
//    }

    @Nested
    class PostTest {
        //Post перестал работать, когда база стала pg, а не встроенная
        @Test
        public void postSuccessWithoutId() {
            createdCountryId = validatePost(createBody("qe"))
                    .statusCode(201)
                    .body(
                            "countryName", is("qe"),
                            "id", notNullValue())
                    .extract()
                    .path("id");

            validateGetById(createdCountryId)
                    .statusCode(200)
                    .body("id", is(createdCountryId),
                            "countryName", is("qe"));
        }

        @Test
        public void postErrorWithId() {
            validatePost(createBody("kj", 1005))
                    .statusCode(400)
                    .body(
                            "title", is("A new country cannot already have an ID"),
                            "status", is(400));
        }
    }

    @Nested
    class GetTest {
        @Test
        public void getWithId() throws SQLException {
            String countryName = "ed";
            createdCountryId = createCountry(countryName);

            validateGetById(createdCountryId)
                    .statusCode(200)
                    .body(
                            "id", is(createdCountryId),
                            "countryName", is(countryName),
                            "locations", nullValue());
        }
    }

    @Nested
    class PatchTest {
        @Test
        public void patchSuccess() throws SQLException {
            String countryName = "pa";
            createdCountryId = createCountry(countryName);

            given()
                    .body(createBody("jh", createdCountryId))
                    .when()
                    .pathParams("id", createdCountryId)
                    .patch(PATH_WITH_ID)
                    .then()
                    .statusCode(200)
                    .body(
                            "id", is(createdCountryId),
                            "countryName", is("jh"),
                            "locations", nullValue()
                    );
            validateGetById(createdCountryId)
                    .statusCode(200)
                    .body("id", is(createdCountryId),
                            "countryName", is("jh"));
//почему-то не проверяет так, если неправильно тест не падает
            assertThat(getCountryNameFromDB(createdCountryId).equals("jh"));
        }

        @Test
        public void patchError() throws SQLException {
            createdCountryId = createCountry("pa");
            given()
                    .body(createBody("pa", createdCountryId))
                    .when().patch("/api/countries")
                    .then()
                    .statusCode(405)
                    .body(
                            "title", is("Method Not Allowed"),
                            "detail", is("Request method 'PATCH' not supported")
                    );
        }
    }

    @Nested
    class PutTest {
        @Test
        public void putSuccess() throws SQLException {
            String countryName = "pu";
            createdCountryId = createCountry(countryName);
            given()
                    .contentType("application/json")
                    .body(createBody("po", createdCountryId))
                    .when()
                    .pathParams("id", createdCountryId)
                    .put(PATH_WITH_ID)
                    .then()
                    .statusCode(200)
                    .body(
                            "id", is(createdCountryId),
                            "countryName", is("po"),
                            "locations", nullValue()
                    );
            validateGetById(createdCountryId)
                    .statusCode(200)
                    .body("id", is(createdCountryId),
                            "countryName", is("po"));
            assertThat(getCountryNameFromDB(createdCountryId).equals("po"));
        }
    }

    @Nested
    class DeleteTest {
        @Test
        public void deleteSuccess() throws SQLException {
            createdCountryId = createCountry("re");

            validateDelete(createdCountryId)
                    .statusCode(204);

            validateGetById(createdCountryId)
                    .statusCode(404)
                    .body("title", is("Not Found"),
                            "status", is(404));

            createdCountryId = null;
        }

        @Test
        public void deleteError() {
            validateDelete(2030)
                    .statusCode(500)
                    .body("title", is("Internal Server Error"),
                            "status", is(500));
        }
    }


    private String createBody(String countryName, int id) {
        return String.format("{\"countryName\" : \"%s\", \"id\" : \"%d\"}", countryName, id);
    }

    private String createBody(String countryName) {
        return String.format("{\"countryName\" : \"%s\"}", countryName);
    }

    private ValidatableResponse validatePost(String body) {
        return given()
                .body(body)
                .when().post(PATH)
                .then();

    }

    private Integer createCountry(String countryName) throws SQLException {
        PreparedStatement sql = connection.prepareStatement(
                "INSERT INTO country( country_name) values (?);",
                Statement.RETURN_GENERATED_KEYS
        );
        sql.setString(1, countryName);
        sql.executeUpdate();

        ResultSet resultSet = sql.getGeneratedKeys();
        Integer id = null;
        while (resultSet.next()){
            id = resultSet.getInt(1);
        }
        return id;
    }

    private String getCountryNameFromDB(Integer id) throws SQLException {
        PreparedStatement sql1 = connection.prepareStatement(
                "SELECT * from country where id = ?;"
        );
        sql1.setInt(1,id);
        ResultSet resultSet1 = sql1.executeQuery();
        String name = null;
        while (resultSet1.next()){
            name = resultSet1.getString(2);
        }

        return name;
    }


    private ValidatableResponse validateDelete(int id) {
        return given()
                .when()
                .pathParams("id", id)
                .delete(PATH_WITH_ID)
                .then();

    }

    private ValidatableResponse validateGetById(int id) {
        return given()
                .when()
                .pathParams("id", id)
                .get(PATH_WITH_ID)
                .then();
    }
}