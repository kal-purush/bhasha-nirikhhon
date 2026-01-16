package handlers.meetings;

import app.App;
import com.google.gson.Gson;
import entities.Meeting;
import io.javalin.http.Context;
import io.javalin.http.Handler;
import org.jetbrains.annotations.NotNull;

public class CreateMeetingHandler implements Handler {
    @Override
    public void handle(@NotNull Context ctx) {
        String json = ctx.body();
        Gson gson = new Gson();
        Meeting meeting = gson.fromJson(json, Meeting.class);

        int saveMeeting = App.meetingService.createMeeting(meeting);

        if (saveMeeting ==201){
            ctx.status(saveMeeting);
            ctx.result("Meeting scheduled");
        }else {
            ctx.status(422);
            ctx.result("Meeting not scheduled, please try again later");
        }
    }
}