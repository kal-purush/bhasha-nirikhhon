package com.lazo.saos.controller;

import com.lazo.saos.app.controller.MainController;
import com.lazo.saos.app.model.Matrix;
import com.lazo.saos.utils.TestUtils;
import org.json.simple.parser.ParseException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import static org.junit.Assert.*;
import static org.junit.Assert.assertNotEquals;

/**
 * Created by Lazo on 2023-12-15
 */

@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
public class SaosControllerIntegrationTest extends TestUtils {

    @Autowired
    private MainController mainController;

    @Test
    public void testComputeSuccessRateWhenMatrixEmpty() {
        var outcome = mainController.computeSuccessRate(new Matrix());
        assertEquals(outcome.getStatusCode(), HttpStatus.BAD_REQUEST);
    }

    @Test
    public void testComputeSuccessRateWhenMatrixIsNotEmpty() throws IOException, ParseException {

        var o = getJSONObject("src/main/resources/text.json");

        var rvLocal = (HashMap<String, Double>) o.get("rv");
        var matrixLocal = (ArrayList<HashMap<String, Boolean>>) o.get("matrix");
        var matrix = getMatrix(matrixLocal, rvLocal);

        ResponseEntity<Double> outcome = mainController.computeSuccessRate(matrix);

        var body = outcome.getBody();

        assertNotNull(body);
        assertNotEquals(body.longValue(), 0.0);
        assertNotEquals(body.longValue(), 1.0);
        assertEquals(outcome.getStatusCode(), HttpStatus.OK);
    }

    @Test
    public void testPrintMatrixWhenMatrixEmpty() {
        ResponseEntity<Matrix> outcome = mainController.printMatrix(new Matrix());
        var body = outcome.getBody();

        assertNull(body);
        assertEquals(outcome.getStatusCode(), HttpStatus.BAD_REQUEST);
    }

    @Test
    public void testPrintMatrixWhenMatrixIsNotEmpty() throws IOException, ParseException {
        var o = getJSONObject("src/main/resources/text.json");

        var matrixLocal = (ArrayList<HashMap<String, Boolean>>) o.get("matrix");
        var matrix = getMatrix(matrixLocal, new HashMap<>());

        ResponseEntity<Matrix> outcome = mainController.printMatrix(matrix);

        var body = outcome.getBody();

        assertNotNull(body);
        assertNotNull(body.getMatrix());
        assertNull(body.getRv());
        assertNotEquals(body.getMatrix().size(), 0);
        assertEquals(outcome.getStatusCode(), HttpStatus.OK);
    }

    @Test
    public void testCalculateWeightsMatrixEmptyAndIndexNull() {
        ResponseEntity<Double> outcome = mainController.calculateWeights(new Matrix(), null);
        var body = outcome.getBody();

        assertNull(body);
        assertEquals(outcome.getStatusCode(), HttpStatus.BAD_REQUEST);
    }

    @Test
    public void testCalculateWeightsMatrixIsNotEmptyAndIndexNull() throws IOException, ParseException {
        var o = getJSONObject("src/main/resources/text.json");

        var matrixLocal = (ArrayList<HashMap<String, Boolean>>) o.get("matrix");
        var matrix = getMatrix(matrixLocal, new HashMap<>());

        ResponseEntity<Double> outcome = mainController.calculateWeights(matrix, null);
        var body = outcome.getBody();

        assertNull(body);
        assertEquals(outcome.getStatusCode(), HttpStatus.BAD_REQUEST);
    }

    @Test
    public void testCalculateWeightsMatrixIsNotEmptyAndIndexNotNull() throws IOException, ParseException {
        var o = getJSONObject("src/main/resources/text.json");

        var matrixLocal = (ArrayList<HashMap<String, Boolean>>) o.get("matrix");
        var matrix = getMatrix(matrixLocal, new HashMap<>());

        ResponseEntity<Double> outcome = mainController.calculateWeights(matrix, 1);
        var body = outcome.getBody();

        assertNotNull(body);
        assertEquals(outcome.getStatusCode(), HttpStatus.OK);
    }



}