package com.chatbot.permit.municipal;

import com.chatbot.permit.municipal.controller.MainController;
import com.chatbot.permit.municipal.db.DBConnection;
import com.chatbot.permit.municipal.model.Maps;
import com.chatbot.permit.municipal.model.Polygons;
import com.chatbot.permit.municipal.repository.MapsRepository;
import com.chatbot.permit.municipal.repository.PolygonsRepository;
import com.chatbot.permit.municipal.service.JsonParsingService;
import org.junit.Test;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.runner.RunWith;
import org.mockito.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.servlet.MockMvc;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

import java.sql.*;
import java.util.Arrays;
import java.util.List;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@RunWith(SpringRunner.class)
@WebMvcTest
public class RetrievePermitInformationTest {

    @Mock
    DBConnection dbConnection;
    @Mock
    Connection connection;
    @Mock
    PreparedStatement preparedStatement;
    @Mock
    ResultSet resultSet;
    @MockBean
    private JsonParsingService jsonParsingService;
    @MockBean
    private MapsRepository mockMapsRepository;
    @MockBean
    private PolygonsRepository mockPolygonsRepository;
    @Autowired
    @InjectMocks
    private MainController mainController;
    @Autowired
    private MockMvc mockMvc;
    MockedStatic mockDriverManager = Mockito.mockStatic(DriverManager.class);

    @BeforeEach
    public void setUp() throws SQLException {
        MockitoAnnotations.initMocks(this);
        // mock data for maps repository
        List<Integer> zoneIds = Arrays.asList(71);
        given(mockMapsRepository.findMapsDistinctBy()).willReturn(zoneIds);
        given(mockMapsRepository.findByFKPOLYGONID(71))
                .willReturn(Arrays.asList(new Maps(71, -118.171751, 34.181911, -118171751, 34181911),
                        new Maps(71, -118.166906, 34.151521, -118166906, 34151521),
                        new Maps(71, -118.164063, 34.149659, -118164063, 34149659),
                        new Maps(71, -118.163817, 34.164540, -118163817, 34164540)));
        // mock data for polygons repository
        given(mockPolygonsRepository.findById(71))
                .willReturn(java.util.Optional.of(new Polygons(71, "OS", "OS")));
        given(mockPolygonsRepository.findZONECODEByPOLYGONID(71))
                .willReturn("OS");
        this.mainController.initMapHandler();
    }

    @AfterEach
    public void clearMocks() {
        mockDriverManager.close();
    }

    @Test
    public void retrievePermitInformationTest() throws Exception {
        // permit test
        String sqlStml =
                "select alu.application_url from allowed_land_use alu join zone_land_use zlu on zlu.id = alu.zone_land_use_id where zlu.description= ? and (alu.zone_id= ? or alu.zone_id='ALL')";
        when(resultSet.next()).thenReturn(true).thenReturn(false);
        when(resultSet.getString("application_url")).thenReturn(
                "https://www.cityofpasadena.net/wp-content/uploads/sites/30/Zoning-Permit-Application.pdf?v=1602628892503");
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(connection.prepareStatement(sqlStml)).thenReturn(preparedStatement);
        mockDriverManager.when((MockedStatic.Verification) DriverManager.getConnection(anyString(), anyString(), anyString())).thenReturn(connection);
        Object requestParameters = new Object() {
            public String action = "remove";
            public String object = "tree";
            public String type = "permit";
            public int zoneID = 71;
            public String webhookType = "retrieveInformation";
        };
        ObjectMapper requestParametersMapper = new ObjectMapper();
        String requestParamsJson = requestParametersMapper.writeValueAsString(requestParameters);

        this.mockMvc
                .perform(post("/umgcchatbot").content(requestParamsJson).contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk()).andExpect(content().json("{'permitUrl': \"https://www.cityofpasadena.net/wp-content/uploads/sites/30/Zoning-Permit-Application.pdf?v=1602628892503\"}"));

        // regulation test
        sqlStml =
                "select alu.procedure_url from allowed_land_use alu join zone_land_use zlu on zlu.id = alu.zone_land_use_id where zlu.description= ? and (alu.zone_id= ? or alu.zone_id='ALL')";
        when(resultSet.next()).thenReturn(true).thenReturn(false);
        when(resultSet.getString("procedure_url")).thenReturn(
                "https://library.municode.com/ca/pasadena/codes/code_of_ordinances?nodeId=TIT8HESA_CH8.52CITRTRPROR_8.52.070PRPRTRRELATRPRPEPP");
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(connection.prepareStatement(sqlStml)).thenReturn(preparedStatement);
        requestParameters = new Object() {
            public String action = "remove";
            public String object = "tree";
            public String type = "regulation";
            public int zoneID = 71;
            public String webhookType = "retrieveInformation";
        };
        requestParametersMapper = new ObjectMapper();
        requestParamsJson = requestParametersMapper.writeValueAsString(requestParameters);

        this.mockMvc
                .perform(post("/umgcchatbot").content(requestParamsJson).contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk()).andExpect(content().json("{'regulationUrl': \"https://library.municode.com/ca/pasadena/codes/code_of_ordinances?nodeId=TIT8HESA_CH8.52CITRTRPROR_8.52.070PRPRTRRELATRPRPEPP\"}"));

        // zone symbol test
        sqlStml =
                "select z.zone_symbol from polygons p join zone z on p.zone_code = z.zone_symbol where p.POLYGON_ID = ?;";
        when(resultSet.next()).thenReturn(true).thenReturn(false);
        when(resultSet.getString("zone_symbol")).thenReturn("OS");
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(connection.prepareStatement(sqlStml)).thenReturn(preparedStatement);

        requestParameters = new Object() {
            public String type = "zoneSymbol";
            public int zoneID = 71;
            public String webhookType = "retrieveZoneSymbol";
        };
        requestParametersMapper = new ObjectMapper();
        requestParamsJson = requestParametersMapper.writeValueAsString(requestParameters);

        this.mockMvc
                .perform(post("/umgcchatbot").content(requestParamsJson).contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk()).andExpect(content().json("{'zoneSymbol': \"OS\"}"));

        // development standards test
        sqlStml = "select * from development_standards ds where ds.zone_id= ?";
        when(resultSet.wasNull()).thenReturn(true).thenReturn(true).thenReturn(true);
        when(resultSet.next()).thenReturn(true).thenReturn(false);
        when(resultSet.getString("additional_standard_url")).thenReturn(null);
        when(resultSet.getString("frontage_and_facades_standards_url")).thenReturn(null);
        when(resultSet.getString("general_standard_url")).thenReturn(
                "https://library.municode.com/ca/pasadena/codes/code_of_ordinances?nodeId=TIT17_ZONING_CODE_ART2ZODIALLAUSZOECST_CH17.26SPPUZODI_17.26.040SPPUDIGEDEST");
        when(resultSet.getString("garden_standard_url")).thenReturn(null);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(connection.prepareStatement(sqlStml)).thenReturn(preparedStatement);

        requestParameters = new Object() {
            public String type = "developmentStandards";
            public int zoneID = 71;
            public String webhookType = "retrieveStandard";
        };
        requestParametersMapper = new ObjectMapper();
        requestParamsJson = requestParametersMapper.writeValueAsString(requestParameters);

        this.mockMvc
                .perform(post("/umgcchatbot").content(requestParamsJson).contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk()).andExpect(content().json("{'additionalStandardURL': \"None\", 'frontageandFacadesStandardsURL': \"None\", 'generalStandardURL': \"https://library.municode.com/ca/pasadena/codes/code_of_ordinances?nodeId=TIT17_ZONING_CODE_ART2ZODIALLAUSZOECST_CH17.26SPPUZODI_17.26.040SPPUDIGEDEST\", 'gardenStandardURL': \"None\"}"));
    }

    /*
    @Test
    public void retrieveRegulationInformationTest() throws Exception {
        String sqlStml =
                "select alu.procedure_url from allowed_land_use alu join zone_land_use zlu on zlu.id = alu.zone_land_use_id where zlu.description= ? and (alu.zone_id= ? or alu.zone_id='ALL')";
        when(resultSet.next()).thenReturn(true).thenReturn(false);
        when(resultSet.getString("procedure_url")).thenReturn(
                "https://library.municode.com/ca/pasadena/codes/code_of_ordinances?nodeId=TIT8HESA_CH8.52CITRTRPROR_8.52.070PRPRTRRELATRPRPEPP");
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(connection.prepareStatement(sqlStml)).thenReturn(preparedStatement);
        mockDriverManager.when((MockedStatic.Verification) DriverManager.getConnection(anyString(), anyString(), anyString())).thenReturn(connection);

        Object requestParameters = new Object() {
            public String action = "remove";
            public String object = "tree";
            public String type = "regulation";
            public int zoneID = 71;
            public String webhookType = "retrieveInformation";
        };
        ObjectMapper requestParametersMapper = new ObjectMapper();
        String requestParamsJson = requestParametersMapper.writeValueAsString(requestParameters);

        this.mockMvc
                .perform(post("/umgcchatbot").content(requestParamsJson).contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk()).andExpect(content().json("{'regulationUrl': \"https://library.municode.com/ca/pasadena/codes/code_of_ordinances?nodeId=TIT8HESA_CH8.52CITRTRPROR_8.52.070PRPRTRRELATRPRPEPP\"}"));
    }

    @Test
    public void retrieveZoneSymbolTest() throws Exception {
        String sqlStml =
                "select z.zone_symbol from polygons p join zone z on p.zone_code = z.zone_symbol where p.POLYGON_ID = ?;";
        when(resultSet.next()).thenReturn(true).thenReturn(false);
        when(resultSet.getString("zone_symbol")).thenReturn("OS");
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(connection.prepareStatement(sqlStml)).thenReturn(preparedStatement);

        Object requestParameters = new Object() {
            public String type = "zoneSymbol";
            public int zoneID = 71;
            public String webhookType = "retrieveZoneSymbol";
        };
        ObjectMapper requestParametersMapper = new ObjectMapper();
        String requestParamsJson = requestParametersMapper.writeValueAsString(requestParameters);

        this.mockMvc
                .perform(post("/umgcchatbot").content(requestParamsJson).contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk()).andExpect(content().json("{'zoneSymbol': \"OS\"}"));
    }

    @Test
    public void retrieveDevelopmentStandardsTest() throws Exception {
        String sqlStml = "select * from development_standards ds where ds.zone_id= ?";
        when(resultSet.wasNull()).thenReturn(true).thenReturn(true).thenReturn(true);
        when(resultSet.next()).thenReturn(true).thenReturn(false);
        when(resultSet.getString("additional_standard_url")).thenReturn(null);
        when(resultSet.getString("frontage_and_facades_standards_url")).thenReturn(null);
        when(resultSet.getString("general_standard_url")).thenReturn(
                "https://library.municode.com/ca/pasadena/codes/code_of_ordinances?nodeId=TIT17_ZONING_CODE_ART2ZODIALLAUSZOECST_CH17.26SPPUZODI_17.26.040SPPUDIGEDEST");
        when(resultSet.getString("garden_standard_url")).thenReturn(null);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(connection.prepareStatement(sqlStml)).thenReturn(preparedStatement);

        Object requestParameters = new Object() {
            public String type = "developmentStandards";
            public int zoneID = 71;
            public String webhookType = "retrieveStandard";
        };
        ObjectMapper requestParametersMapper = new ObjectMapper();
        String requestParamsJson = requestParametersMapper.writeValueAsString(requestParameters);

        this.mockMvc
                .perform(post("/umgcchatbot").content(requestParamsJson).contentType(MediaType.APPLICATION_JSON))
                .andExpect(status().isOk()).andExpect(content().json("{'additionalStandardURL': \"None\", 'frontageandFacadesStandardsURL': \"None\", 'generalStandardURL': \"https://library.municode.com/ca/pasadena/codes/code_of_ordinances?nodeId=TIT17_ZONING_CODE_ART2ZODIALLAUSZOECST_CH17.26SPPUZODI_17.26.040SPPUDIGEDEST\", 'gardenStandardURL': \"None\"}"));
    }
    */
}