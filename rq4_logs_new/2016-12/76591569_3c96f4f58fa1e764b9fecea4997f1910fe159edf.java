package com.cubeproblem;

import com.cubeproblem.domain.CubeIteration;
import com.cubeproblem.domain.CubeOperation;
import com.cubeproblem.domain.CubeProblem;
import com.cubeproblem.domain.IterationResult;
import com.cubeproblem.domain.Solution;
import org.glassfish.grizzly.http.server.HttpServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.assertEquals;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;

public class CubeProblemTest {

    private HttpServer server;
    private WebTarget target;

    @Before
    public void setUp() throws Exception {
        // start the server
        server = Main.startServer();
        // create the client
        Client c = ClientBuilder.newClient();

        // uncomment the following line if you want to enable
        // support for JSON in the client (you also have to uncomment
        // dependency on jersey-media-json module in pom.xml and Main.startServer())
        // --
        // c.configuration().enable(new org.glassfish.jersey.media.json.JsonJaxbFeature());

        target = c.target(Main.BASE_URI);
    }

    @After
    public void tearDown() throws Exception {
        server.stop();
    }

    /**
     * Test to see that the message "Got it!" is sent in the response.
     */
    @Test
    public void testCubeProblem() {

        CubeProblem cubeProblem = new CubeProblem();
        cubeProblem.setNumTestCases(2);

        List<CubeIteration> iterations = new ArrayList<>();

        CubeIteration cubeIteration = new CubeIteration();
        cubeIteration.setN(4);
        cubeIteration.setM(5);
        List<CubeOperation> operations = new ArrayList<>();
        CubeOperation cubeOperation = new CubeOperation();
        cubeOperation.setOperation("UPDATE 2 2 2 4");
        CubeOperation cubeOperation2 = new CubeOperation();
        cubeOperation2.setOperation("QUERY 1 1 1 3 3 3");
        CubeOperation cubeOperation3 = new CubeOperation();
        cubeOperation3.setOperation("UPDATE 1 1 1 23");
        CubeOperation cubeOperation4 = new CubeOperation();
        cubeOperation4.setOperation("QUERY 2 2 2 4 4 4");
        CubeOperation cubeOperation5 = new CubeOperation();

        operations.add(cubeOperation);
        operations.add(cubeOperation2);
        operations.add(cubeOperation3);
        operations.add(cubeOperation4);
        operations.add(cubeOperation5);
        cubeIteration.setCubeOperations(operations);

        CubeIteration cubeIteration2 = new CubeIteration();
        cubeIteration2.setN(2);
        cubeIteration2.setM(4);
        List<CubeOperation> operations2 = new ArrayList<>();
        cubeOperation5.setOperation("QUERY 1 1 1 3 3 3");
        CubeOperation cubeOperation6 = new CubeOperation();
        cubeOperation6.setOperation("UPDATE 2 2 2 1");
        CubeOperation cubeOperation7 = new CubeOperation();
        cubeOperation7.setOperation("QUERY 1 1 1 1 1 1");
        CubeOperation cubeOperation8 = new CubeOperation();
        cubeOperation8.setOperation("QUERY 1 1 1 2 2 2");
        CubeOperation cubeOperation9 = new CubeOperation();
        cubeOperation9.setOperation("QUERY 2 2 2 2 2 2");

        operations2.add(cubeOperation6);
        operations2.add(cubeOperation7);
        operations2.add(cubeOperation8);
        operations2.add(cubeOperation9);
        cubeIteration2.setCubeOperations(operations2);

        iterations.add(cubeIteration);
        iterations.add(cubeIteration2);

        cubeProblem.setIterations(iterations);


        Response response = target.path("cubeproblem").request().post(Entity.entity(cubeProblem, MediaType.APPLICATION_JSON_TYPE));
        IterationResult iterationResult = response.readEntity(IterationResult.class);
        assertEquals("Should return status 200", 200, response.getStatus());
        assertEquals(9, iterationResult.getSolutions().size());

        for (Solution solution: iterationResult.getSolutions()) {

            System.out.println(solution.getOperation() + ": " + solution.getResult());

        }

    }

}