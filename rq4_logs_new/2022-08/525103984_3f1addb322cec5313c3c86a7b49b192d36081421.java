package handlers.complaints;

import app.App;
import com.google.gson.Gson;
import entities.Complaint;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import org.jetbrains.annotations.NotNull;

public class UpdateComplaintByIdHandler implements Handler {
    @Override
    public void handle(@NotNull Context ctx) {
        int complaintId= Integer.parseInt((ctx.pathParam("complaintId")));
        String json = ctx.body();
        Gson gson = new Gson();
        Complaint complaint = gson.fromJson(json, Complaint.class);

        int result = App.complaintService.updateComplaintById(complaintId,complaint.getPriority(),complaint.getMeeting());

        ctx.status(result);

    }
}