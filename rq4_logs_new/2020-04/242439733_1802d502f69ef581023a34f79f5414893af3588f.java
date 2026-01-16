package umgc.city.team1.services;

import freemarker.template.TemplateException;
import lombok.Data;
import org.assertj.core.api.Assertions;
import org.junit.Before;
import org.junit.Rule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.rules.ExpectedException;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import umgc.city.team1.exceptions.*;
import umgc.city.team1.models.*;
import umgc.city.team1.models.incoming.MapShape;
import umgc.city.team1.models.incoming.UseCaseDto;
import umgc.city.team1.models.incoming.UserAccount;
import umgc.city.team1.models.outgoing.MapCase;
import umgc.city.team1.models.outgoing.MapZone;
import umgc.city.team1.repositories.*;
import umgc.city.team1.utilities.SendGridEmailService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

public class ZoningProjectServiceTest {
    ZoningProjectService zoningProjectService = null;
    CityRepository cityRepository = Mockito.mock(CityRepository.class);
    CityUserRepository cityUserRepository = Mockito.mock(CityUserRepository.class);
    ZoneRepository zoneRepository = Mockito.mock(ZoneRepository.class);
    ZoneLandUseRepository zoneLandUseRepository = Mockito.mock(ZoneLandUseRepository.class);
    AllowedLandUseRepository allowedLandUseRepository = Mockito.mock(AllowedLandUseRepository.class);
    DevelopmentStandardsRepository developmentStandardsRepository = Mockito.mock(DevelopmentStandardsRepository.class);
    final AuthoritiesRepository authoritiesRepository = Mockito.mock(AuthoritiesRepository.class);
    SendGridEmailService sendGridEmailService = Mockito.mock(SendGridEmailService.class);

    @BeforeEach
    public void setUp() {
        zoningProjectService = new ZoningProjectService(cityRepository, cityUserRepository, zoneRepository, zoneLandUseRepository, allowedLandUseRepository, developmentStandardsRepository, authoritiesRepository, sendGridEmailService);
    }

    @Test
    public void test_Existing_Zones_By_City_Id() throws ZoneNotFoundException {
        //Arrange
        UUID cityId = UUID.randomUUID();
        Zone zone = new Zone();
        List<Zone> zones = new ArrayList<>();
        zones.add(zone);
        when(zoneRepository.findAllByCity(cityId)).thenReturn(zones);

        //Act
        zoningProjectService.getZonesByCityId(cityId);

        //Assert
        assertEquals(1, zones.size());
    }

    @Test
    public void test_Non_Existing_Zones_By_City_Id() throws ZoneNotFoundException {
        //Arrange
        UUID cityId = UUID.randomUUID();
        List<Zone> zones = new ArrayList<>();
        when(zoneRepository.findAllByCity(cityId)).thenReturn(zones);

        //Act
        zoningProjectService.getZonesByCityId(cityId);

        //Assert
        assertEquals(0, zones.size());
    }

    @Test
    public void test_Existing_UseCases_By_Zone() throws UseCaseNotFoundException {
        //Arrange
        UUID zoneId = UUID.randomUUID();
        UseCaseDto useCaseDto = new UseCaseDto();
        List<UseCaseDto> useCaseDtos = new ArrayList<>();
        useCaseDtos.add(useCaseDto);
        when(zoneRepository.findUseCaseByZoneId(zoneId)).thenReturn(useCaseDtos);

        //Act
        zoningProjectService.getUseCasesByZone(zoneId);

        //Assert
        assertEquals(1, useCaseDtos.size());
    }

    @Test
    public void test_Non_Existing_UseCases_By_Zone() throws ZoneNotFoundException {
        //Arrange
        UUID zoneId = UUID.randomUUID();
        List<UseCaseDto> useCaseDtos = new ArrayList<>();
        when(zoneRepository.findUseCaseByZoneId(zoneId)).thenReturn(useCaseDtos);

        //Act
        zoningProjectService.getZonesByCityId(zoneId);

        //Assert
        assertEquals(0, useCaseDtos.size());
    }

    @Test
    public void test_Existing_UseCases_By_City() throws UseCaseNotFoundException {
        //Arrange
        UUID cityId = UUID.randomUUID();
        UseCaseDto useCaseDto = new UseCaseDto();
        List<UseCaseDto> useCaseDtos = new ArrayList<>();
        useCaseDtos.add(useCaseDto);
        when(zoneRepository.findUseCaseByCityId(cityId)).thenReturn(useCaseDtos);

        //Act
        zoningProjectService.getUseCasesByCity(cityId);

        //Assert
        assertEquals(1, useCaseDtos.size());
    }

    @Test
    public void test_Non_Existing_UseCases_By_City() throws UseCaseNotFoundException {
        //Arrange
        UUID cityId = UUID.randomUUID();
        List<UseCaseDto> useCaseDtos = new ArrayList<>();
        when(zoneRepository.findUseCaseByCityId(cityId)).thenReturn(useCaseDtos);

        //Act
        zoningProjectService.getUseCasesByCity(cityId);

        //Assert
        assertEquals(0, useCaseDtos.size());
    }

    @Test
    public void test_Existing_City_By_Id() throws  CityNotFoundException {
        //Arrange
        UUID cityId = UUID.randomUUID();
        City city = new City();
        city.setName("TestCity");
        when(cityRepository.findById(cityId)).thenReturn(Optional.of(city));

        //Act
        Optional<City> result = zoningProjectService.getCityById(cityId);

        //Assert
        assertEquals(true, result.isPresent());
        assertEquals("TestCity", result.get().getName());
    }

    @Test
    public void test_Non_Existing_City_By_Id() throws CityNotFoundException {
        //Arrange
        UUID cityId = UUID.randomUUID();
        when(cityRepository.findById(cityId)).thenReturn(Optional.empty());

        //Act
        Optional<City> result = zoningProjectService.getCityById(cityId);

        //Assert
        assertEquals(false, result.isPresent());
    }

    @Test
    public void test_Valid_Account() {
        //Arrange
        UserAccount userAccount = new UserAccount();
        userAccount.setEmail("Test@gmail.com");
        userAccount.setPassword("1234");
        CityUser cityUser = new CityUser();
        cityUser.setEmailAddress("Test@gmail.com");
        cityUser.setPassword("1234");
        City city = new City();
        city.setId(UUID.randomUUID());
        cityUser.setCity(city);
        when(cityUserRepository.getUserByEmail(userAccount.getEmail())).thenReturn(cityUser);

        //Act
        UserAccount result = zoningProjectService.VerifyUserAccount(userAccount);

        //Assert
        assertEquals(userAccount.getEmail(), result.getEmail());
    }

    @Test
    public void test_Invalid_Password() {
        //Arrange
        UserAccount userAccount = new UserAccount();
        userAccount.setEmail("Test@gmail.com");
        userAccount.setPassword("1234");
        CityUser cityUser = new CityUser();
        cityUser.setEmailAddress("Test@gmail.com");
        cityUser.setPassword("6789");
        when(cityUserRepository.getUserByEmail(userAccount.getEmail())).thenReturn(cityUser);

        //Act
        Exception e = null;
        try {
            zoningProjectService.VerifyUserAccount(userAccount);
        } catch (CityUserNotFoundException ex) {
            e = ex;
        }

        //Assert
        assertTrue(e instanceof CityUserNotFoundException);
    }

    @Test
    public void test_Non_Existing_User() {
        //Arrange
        UserAccount userAccount = new UserAccount();
        userAccount.setEmail("Test@gmail.com");
        userAccount.setPassword("1234");
        CityUser cityUser = null;
        when(cityUserRepository.getUserByEmail(userAccount.getEmail())).thenReturn(cityUser);

        //Act
        Exception e = null;
        try {
            zoningProjectService.VerifyUserAccount(userAccount);
        } catch (CityUserNotFoundException ex) {
            e = ex;
        }

        //Assert
        assertTrue(e instanceof CityUserNotFoundException);
    }

    @Test
    public void test_Successful_Account_Creation() throws ObjectCreationFailedException {
        //Arrange
        UUID cityUserId = UUID.randomUUID();
        UserAccount userAccount = new UserAccount();
        userAccount.setFirstName("Test");
        userAccount.setLastName("User");
        userAccount.setEmail("Test@gmail.com");
        userAccount.setPassword("1234");
        userAccount.setAuthoritiesId(UUID.randomUUID());
        userAccount.setCity("TestCity");
        userAccount.setState("TestState");
        City city = new City();
        city.setId(UUID.randomUUID());
        CityUser cityUser = new CityUser();
        cityUser.setId(cityUserId);
        cityUser.setCity(city);
        cityUser.setId(cityUserId);
        Authorities authorities = new Authorities();
        authorities.setId(UUID.randomUUID());
        when(authoritiesRepository.getAuthorityByName(anyString())).thenReturn(authorities);
        when(cityUserRepository.save(any(CityUser.class))).thenReturn(cityUser);
        when(cityRepository.save(any(City.class))).thenReturn(city);

        //Act
        CityUser result = zoningProjectService.createUserAccount(userAccount);

        //Assert
        //Passes if no exception
    }

    @Test
    public void test_Unsuccessful_Account_Creation() throws ObjectCreationFailedException {
        //Arrange
        UUID cityUserId = UUID.randomUUID();
        UserAccount userAccount = new UserAccount();
        userAccount.setEmail("Test@gmail.com");
        userAccount.setPassword("1234");
        CityUser cityUser = new CityUser();
        cityUser.setId(cityUserId);
        City city = new City();
        city.setId(UUID.randomUUID());
        cityUser.setCity(city);
        when(cityUserRepository.save(any(CityUser.class))).thenReturn(cityUser);
        when(cityRepository.save(any(City.class))).thenReturn(city);

        //Act
        Exception e = null;
        try {
            zoningProjectService.createUserAccount(userAccount);
        } catch (ObjectCreationFailedException ex) {
            e = ex;
        }

        //Assert
        assertTrue(e instanceof ObjectCreationFailedException);
    }

    @Test
    public void test_Successful_sendAdminUserCredentialsInEmail_Valid_Data() throws IOException, TemplateException {
        //Arrange
        UserAccount userAccount = new UserAccount();
        userAccount.setFirstName("Test");
        userAccount.setLastName("User");
        userAccount.setEmail("Test@gmail.com");
        userAccount.setPassword("1234");
        CityUser cityUser = new CityUser();
        cityUser.setEmailAddress("Test@gmail.com");
        when(cityUserRepository.getUserByEmail("Test@gmail.com")).thenReturn(cityUser);

        //Act
        zoningProjectService.sendAdminUserCredentialsInEmail(userAccount);

        //Assert
        //Passes if no exception
    }

    @Test
    public void test_Unsuccessful_SendAdminUserCredentialsInEmail_Missing_Data() {
        //Arrange
        UserAccount userAccount = new UserAccount();
        CityUser cityUser = new CityUser();
        when(cityUserRepository.getUserByEmail("Test@gmail.com")).thenReturn(cityUser);

        //Act
        Exception e = null;
        try {
            zoningProjectService.sendAdminUserCredentialsInEmail(userAccount);
        } catch (Exception ex) {
            e = ex;
        }

        //Assert
        assertTrue(e instanceof CityUserNotFoundException);
    }

    @Test
    public void test_Null_Input_getPasadenaZoneData() throws ZoneNotFoundException {
        //Arrange
        MapShape mapShape = new MapShape();
        when(zoneRepository.findUseCaseByZoneSymbol(mapShape.getZoneCode())).thenReturn(new MapZone());

        //Act
        MapCase result = zoningProjectService.getPasadenaZoneData(mapShape);

        //Assert
        assertNull(result.getOverlayCode());
    }

    @Test
    public void test_Valid_OverlayCode_Input_getPasadenaZoneData() throws ZoneNotFoundException {
        //Arrange
        MapShape mapShape = new MapShape();
        mapShape.setOverlayCode("PK-LD");
        MapZone mapZone = new MapZone();
        mapZone.setZoneSymbol("PK-LD");
        mapZone.setGeneralStandardsURL("https://library.municode.com/ca/pasadena/codes/code_of_ordinances?nodeId=TIT17_ZONING_CODE_ART2ZODIALLAUSZOECST_CH17.28OVZODI");
        when(zoneRepository.findUseCaseByZoneSymbol(mapShape.getZoneCode())).thenReturn(mapZone);
        //Act
        MapCase result = zoningProjectService.getPasadenaZoneData(mapShape);

        //Assert
        assertEquals("PK-LD", result.getOverlayCode());
        assertEquals("https://library.municode.com/ca/pasadena/codes/code_of_ordinances?nodeId=TIT17_ZONING_CODE_ART2ZODIALLAUSZOECST_CH17.28OVZODI", result.getOverlayGeneralStandardsURL());
    }

    @Test
    public void test_Null_City_createUseCase() throws UseCaseNotFoundException, CityNotFoundException {
        //Arrange
        UseCaseDto useCaseDto = new UseCaseDto();

        //Act
        Exception e = null;
        try {
            zoningProjectService.createUseCase(useCaseDto);
        } catch (Exception ex) {
            e = ex;
        }

        //Assert
        assertTrue(e instanceof CityNotFoundException);
    }

    @Test
    public void test_Valid_createUseCase() throws UseCaseNotFoundException, CityNotFoundException {
        //Arrange
        UUID cityId = UUID.randomUUID();
        UseCaseDto useCaseDto = new UseCaseDto();
        useCaseDto.setCityId(cityId);
        City city = new City();
        city.setId(cityId);
        city.setName("TestCity");
        city.setState("TestState");
        when(cityRepository.findById(city.getId())).thenReturn(Optional.of(city));
        when(zoningProjectService.getCityById(city.getId())).thenReturn(Optional.of(city));

        //Act
        zoningProjectService.createUseCase(useCaseDto);

        //Assert
        //Passes if no exception
    }

    @Test
    public void test__editUseCase() throws UseCaseNotFoundException {
        //Arrange
        UseCaseDto useCaseDto = new UseCaseDto();
        Zone zone = new Zone();
        DevelopmentStandards developmentStandards = new DevelopmentStandards();
        ZoneLandUse zoneLandUse = new ZoneLandUse();
        AllowedLandUse allowedLandUse = new AllowedLandUse();
        when(zoneRepository.findById(useCaseDto.getZoneId())).thenReturn(Optional.of(zone));
        when(developmentStandardsRepository.findByZoneId(useCaseDto.getZoneId())).thenReturn(Optional.of(developmentStandards));
        when(zoneLandUseRepository.findById(useCaseDto.getZoneLandUseId())).thenReturn(Optional.of(zoneLandUse));
        when(allowedLandUseRepository.findAllByZoneandZoneLandUse(useCaseDto.getZoneId(), useCaseDto.getZoneLandUseId())).thenReturn(Optional.of(allowedLandUse));

        //Act
        zoningProjectService.editUseCase(useCaseDto);

        //Assert
        //Passes if no exception
    }
}