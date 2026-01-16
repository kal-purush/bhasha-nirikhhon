package app;

import daos.complaint.ComplaintDaoPostgres;
import handlers.complaints.CreateComplaintHandler;
import io.javalin.Javalin;
import services.complaints.ComplaintServices;
import services.complaints.ComplaintServicesLocal;

public class App {
    public static ComplaintServices complaintService = new ComplaintServicesLocal(new ComplaintDaoPostgres());

    public static void main(String[] args){
        //add Javalin
        Javalin app = Javalin.create();

        //Complaint Handlers
        CreateComplaintHandler createComplaintHandler = new CreateComplaintHandler();

        //Call Complaint Handlers
        app.post("/complaints", createComplaintHandler);

        //start app
        app.start();

    }
}