package servicesTests;

import daos.complaint.ComplaintDao;
import entities.Complaint;
import entities.Level;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import services.complaints.ComplaintServices;
import services.complaints.ComplaintServicesImpl;

import java.util.ArrayList;

import static org.mockito.Mockito.when;

public class ComplaintSevicesTests {


    ComplaintDao complaintDaoMock = Mockito.mock(ComplaintDao.class);
    ComplaintServices complaintServices = new ComplaintServicesImpl(complaintDaoMock);
    Complaint test = new Complaint ("john", "Smith", "Food Happened");
    @Test
    void create_complaint_service_test_standard(){

        when(complaintDaoMock.createComplaint(test)).thenReturn(201);
        int result = complaintServices.createComplaint(test);

        Assertions.assertEquals(201, result);
    }
    @Test
    void get_all_complaints_service_test_standard(){
        ArrayList<Complaint> testList= new ArrayList<>();
        testList.add(test);

        when(complaintDaoMock.getAllComplaints()).thenReturn(testList);
        ArrayList<Complaint> result = complaintServices.getAllComplaints();

        Assertions.assertEquals(1, result.size());
    }

    @Test
    void update_complaint_by_id_service_test_standard(){
        int testId = 200;
        Level testLevel = test.getPriority();
        int testMeeting = 1;

        when(complaintDaoMock.updateComplaintById(testId,testLevel,testMeeting)).thenReturn(200);
        int result = complaintServices.updateComplaintById(testId, testLevel, testMeeting);

        Assertions.assertEquals(200, result);
    }
}