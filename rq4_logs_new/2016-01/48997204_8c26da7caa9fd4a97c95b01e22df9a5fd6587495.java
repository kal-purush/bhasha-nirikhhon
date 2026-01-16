package no.bacheloroppgaven.android.learningbasics4.services;

import android.app.IntentService;
import android.content.Intent;

import com.parse.ParseException;
import com.parse.ParseObject;
import com.parse.ParseQuery;

import java.util.ArrayList;
import java.util.List;

public class ActionExecutorService extends IntentService {

    /**
     * Creates an IntentService.
     * Invoked by your subclass's constructor.
     */
    public ActionExecutorService() {
        super("ActionExecutorService");
    }

    @Override
    protected void onHandleIntent(Intent intent) {
        // Bundle extras = intent.getExtras(); // For sending service parameters
        List<String> actions = getActiveActions();
        Intent broadcastedIntent = new Intent();

        broadcastedIntent.putExtra("activeActions", actions.toArray());
        sendBroadcast(intent);
    }

    private List<String> getActiveActions() {
        List<String> activeActions = new ArrayList<>();
        ParseQuery<ParseObject> query = new ParseQuery<>("Action");

        try {
            for (ParseObject parseObject : query.find()) {
                if (parseObject.getBoolean("active")) {
                    activeActions.add(parseObject.getString("actionName"));
                }
            }
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return activeActions;
    }
}