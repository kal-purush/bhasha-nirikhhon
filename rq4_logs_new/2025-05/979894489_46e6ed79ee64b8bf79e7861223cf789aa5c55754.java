package org.example.atharvolunteeringplatform;

import org.example.atharvolunteeringplatform.Model.Opportunity;
import org.example.atharvolunteeringplatform.Model.Organization;
import org.example.atharvolunteeringplatform.Repository.OpportunityRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.*;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(MockitoExtension.class)
public class OpportunityRepositoryTest {

    @Mock
    OpportunityRepository opportunityRepository;

    Opportunity opp1, opp2, opp3;
    Organization org;

    List<Opportunity> opportunities;

    @BeforeEach
    void setUp() {
        org = new Organization(); // تأكد أنك تجهز كائن المنظمة
        org.setId(1);
        org.setName("Test Org");

        opp1 = new Opportunity();
        opp1.setId(1);
        opp1.setTitle("Opportunity 1");
        opp1.setTypeOpportunity("Volunteering");
        opp1.setGender("Both");
        opp1.setDescription("Description 1");
        opp1.setStartDate(LocalDate.now());
        opp1.setEndDate(LocalDate.now().plusDays(5));
        opp1.setHours(10);
        opp1.setStudentCapacity(100);
        opp1.setLocation("Remote");
        opp1.setImagePath("image1.png");
        opp1.setStatus("open");
        opp1.setCreatedAt(LocalDateTime.now().minusDays(2));
        opp1.setOrganization(org);

        opp2 = new Opportunity();
        opp2.setId(2);
        opp2.setTitle("Opportunity 2");
        opp2.setTypeOpportunity("Internship");
        opp2.setGender("Male");
        opp2.setDescription("Description 2");
        opp2.setStartDate(LocalDate.now());
        opp2.setEndDate(LocalDate.now().plusDays(3));
        opp2.setHours(20);
        opp2.setStudentCapacity(80);
        opp2.setLocation("Onsite");
        opp2.setImagePath("image2.png");
        opp2.setStatus("closed");
        opp2.setCreatedAt(LocalDateTime.now().minusDays(1));
        opp2.setOrganization(org);

        opp3 = new Opportunity();
        opp3.setId(3);
        opp3.setTitle("Opportunity 3");
        opp3.setTypeOpportunity("Volunteering");
        opp3.setGender("Female");
        opp3.setDescription("Description 3");
        opp3.setStartDate(LocalDate.now());
        opp3.setEndDate(LocalDate.now().plusDays(7));
        opp3.setHours(15);
        opp3.setStudentCapacity(60);
        opp3.setLocation("Remote");
        opp3.setImagePath("image3.png");
        opp3.setStatus("open");
        opp3.setCreatedAt(LocalDateTime.now());
        opp3.setOrganization(org);

        opportunities = new ArrayList<>();
        opportunities.add(opp1);
        opportunities.add(opp2);
        opportunities.add(opp3);
    }

    @Test
    public void testFindOpportunityById() {
        when(opportunityRepository.findOpportunityById(1)).thenReturn(opp1);

        Opportunity result = opportunityRepository.findOpportunityById(1);
        assertEquals("Opportunity 1", result.getTitle());
        verify(opportunityRepository, times(1)).findOpportunityById(1);
    }

    @Test
    public void testFindOpenOpportunitiesByOrganizationId() {
        List<Opportunity> openOpportunities = List.of(opp1, opp3);
        when(opportunityRepository.findOpenOpportunitiesByOrganizationId(1)).thenReturn(openOpportunities);

        List<Opportunity> result = opportunityRepository.findOpenOpportunitiesByOrganizationId(1);
        assertEquals(2, result.size());
        assertTrue(result.stream().allMatch(o -> o.getStatus().equals("open")));
        verify(opportunityRepository, times(1)).findOpenOpportunitiesByOrganizationId(1);
    }

    @Test
    public void testFindByTypeOpportunityAndStatus() {
        List<Opportunity> volunteeringOpportunities = List.of(opp1, opp3);
        when(opportunityRepository.findByTypeOpportunityAndStatus("Volunteering", "open")).thenReturn(volunteeringOpportunities);

        List<Opportunity> result = opportunityRepository.findByTypeOpportunityAndStatus("Volunteering", "open");
        assertEquals(2, result.size());
        assertTrue(result.stream().allMatch(o -> o.getTypeOpportunity().equals("Volunteering")));
        verify(opportunityRepository, times(1)).findByTypeOpportunityAndStatus("Volunteering", "open");
    }

    @Test
    public void testFindByLocationAndStatus() {
        List<Opportunity> remoteOpportunities = List.of(opp1, opp3);
        when(opportunityRepository.findByLocationAndStatus("Remote", "open")).thenReturn(remoteOpportunities);

        List<Opportunity> result = opportunityRepository.findByLocationAndStatus("Remote", "open");
        assertEquals(2, result.size());
        assertTrue(result.stream().allMatch(o -> o.getLocation().equals("Remote")));
        verify(opportunityRepository, times(1)).findByLocationAndStatus("Remote", "open");
    }

    @Test
    public void testFindAllByOrderByHoursDesc() {
        List<Opportunity> sortedByHours = List.of(opp2, opp3, opp1); // hours: 20, 15, 10
        when(opportunityRepository.findAllByOrderByHoursDesc()).thenReturn(sortedByHours);

        List<Opportunity> result = opportunityRepository.findAllByOrderByHoursDesc();
        assertEquals(3, result.size());
        assertEquals(20, result.get(0).getHours());
        verify(opportunityRepository, times(1)).findAllByOrderByHoursDesc();
    }
}