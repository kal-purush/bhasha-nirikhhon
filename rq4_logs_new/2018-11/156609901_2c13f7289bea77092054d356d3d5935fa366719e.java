package edu.iss.gitcloneteam.weatherobserver;

import edu.iss.gitcloneteam.weatherobserver.dao.MeasurementDao;
import edu.iss.gitcloneteam.weatherobserver.model.Measurement;
import edu.iss.gitcloneteam.weatherobserver.services.MeasurementServiceImpl;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.LocalDateTime;

import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@SpringBootTest
public class MeasurementServiceMockTests {

    @Mock
    private MeasurementDao measurementDaoMock;

    @InjectMocks
    private MeasurementServiceImpl measurementService;

    @Before
    public void setup(){
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void getLastMeasurementTest() {
        LocalDateTime measurementTimeCurrent = LocalDateTime.now();
        Measurement lastMeasurement = new Measurement();
        lastMeasurement.setMeasurementTime(measurementTimeCurrent);

        Mockito.when(measurementDaoMock.getLastMeasurement())
                .thenReturn(lastMeasurement);

        Measurement actualMeasurement = measurementService.getLastMeasurement();
        assertTrue(actualMeasurement.getMeasurementTime().isEqual(measurementTimeCurrent));
    }


}