package pl.maxmati.tobiasz.mmos.bread.activity;

import android.os.AsyncTask;

import pl.maxmati.tobiasz.mmos.bread.api.APIConnector;
import pl.maxmati.tobiasz.mmos.bread.api.charge.Charge;
import pl.maxmati.tobiasz.mmos.bread.api.charge.ChargeManager;
import pl.maxmati.tobiasz.mmos.bread.api.session.SessionException;
import pl.maxmati.tobiasz.mmos.bread.api.session.SessionManager;

/**
 * Created by mmos on 21.02.16.
 *
 * @author mmos
 */
public class CreateChargeTask extends AsyncTask<Void, Void, Void> {
    private final ProgressBarActivity activity;
    private final Charge charge;

    public CreateChargeTask(ProgressBarActivity activity, Charge charge) {
        this.activity = activity;
        this.charge = charge;
    }

    @Override
    protected Void doInBackground(Void... voids) {
        try {
            ChargeManager.create(new APIConnector(activity, SessionManager.restoreSession
                    (activity)), charge);
        } catch (SessionException e) {
            e.printStackTrace();
        }
        return null;
    }
}