package vvv.gatepass;

import android.app.Dialog;
import android.app.ProgressDialog;
import android.content.Context;
import android.os.AsyncTask;
import android.os.Bundle;
import android.preference.PreferenceManager;
import android.view.View;
import android.view.Window;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;
import android.widget.Toast;

import org.json.JSONException;
import org.json.JSONObject;

import java.util.Calendar;
import java.util.HashMap;

/**
 * Created by Pradumn K Mahanta on 29-03-2016.
 */
public class CustomDialog extends Dialog implements android.view.View.OnClickListener{

    public Context mContext;
    public Dialog d;
    public Button btn_yes, btn_no;

    EditText rReason;

    UpdateRequest updateRequest;

    TextView fNameI, BatchII, inTime, inDate, outTime, visitPurpose, visitPLace, outDate, txt_dia;
    String acc_type, acc_name;

    public GatepassListViewItem mItem;

    public CustomDialog(Context mContext, GatepassListViewItem mItem) {
        super(mContext);
        this.mContext = mContext;
        this.mItem = mItem;
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        requestWindowFeature(Window.FEATURE_NO_TITLE);
        setContentView(R.layout.custom_dialog);

        AppData.LoggedInUser = PreferenceManager.getDefaultSharedPreferences(mContext);
        acc_type = AppData.LoggedInUser.getString("rUserType", "");
        acc_name = AppData.LoggedInUser.getString("rFullName", "");

        btn_yes = (Button) findViewById(R.id.btn_yes);
        btn_no = (Button) findViewById(R.id.btn_no);
        btn_yes.setOnClickListener(this);
        btn_no.setOnClickListener(this);

        fNameI = (TextView) findViewById(R.id.fNameII);
        fNameI.setText(mItem.gp_StudentName);

        txt_dia = (TextView) findViewById(R.id.txt_dia);

        if(acc_type.equals("STUDENT")){
            btn_yes.setText("CLOSE");
            btn_no.setText("CANCEL");
            txt_dia.setText("Your Gatepass");
        }

        updateRequest = new UpdateRequest(mContext);


        inTime = (TextView) findViewById(R.id.inTime);
        inTime.setText(mItem.gp_InTime);

        inDate = (TextView) findViewById(R.id.inDate);
        inDate.setText(mItem.gp_InDate);

        outTime = (TextView) findViewById(R.id.outTime);
        outTime.setText(mItem.gp_OutTime);

        outDate = (TextView) findViewById(R.id.outDate);
        outDate.setText(mItem.gp_OutDate);

        visitPLace = (TextView) findViewById(R.id.visitPLace);
        visitPLace.setText(mItem.gp_VisitPlace);

        visitPurpose = (TextView) findViewById(R.id.visitPurpose);
        visitPurpose.setText(mItem.gp_Purpose);

        BatchII = (TextView) findViewById(R.id.BatchII);
        BatchII.setText(mItem.gp_EnrollmentNo);

        rReason = (EditText) findViewById(R.id.rReason);
    }

    @Override
    public void onClick(View v) {
        switch (v.getId()) {
            case R.id.btn_yes:
                rReason.getText().toString();
                if(acc_type.equals("STUDENT")){
                    dismiss();
                }else {
                    updateRequest.execute(btn_yes.getText().toString() + "d", rReason.getText().toString(), mItem.gp_GatepassNumber);
                    dismiss();
                }
                break;
            case R.id.btn_no:
                rReason.getText().toString();
                if(acc_type.equals("STUDENT")){
                    updateRequest.execute(btn_no.getText().toString() + "led", rReason.getText().toString(), mItem.gp_GatepassNumber);
                }else {
                    updateRequest.execute(btn_no.getText().toString() + "ed", rReason.getText().toString(), mItem.gp_GatepassNumber);
                    dismiss();
                }
                break;
            default:
                break;
        }
        dismiss();
    }


    class UpdateRequest extends AsyncTask<String, String, JSONObject> {

        JSONParser jsonParser = new JSONParser();
        private ProgressDialog pDialog;
        Context ctxt;

        UpdateRequest(Context ctx){
            ctxt = ctx;
        }

        @Override
        protected void onPreExecute() {
            pDialog = new ProgressDialog(ctxt);
            pDialog.setMessage("Updating Request...");
            pDialog.setIndeterminate(false);
            pDialog.setCancelable(true);
            pDialog.show();
        }

        @Override
        protected JSONObject doInBackground(String... args) {
            try {
                HashMap<String, String> params = new HashMap<>();
                params.put("request_status", args[0]);
                params.put("reason", args[1]);
                params.put("gatepass_number", args[2]);
                params.put("approved_by", "dummy.warden");

                Calendar mcurrentTime = Calendar.getInstance();
                int hour = mcurrentTime.get(Calendar.HOUR_OF_DAY);
                int minute = mcurrentTime.get(Calendar.MINUTE);
                int seconds = mcurrentTime.get(Calendar.SECOND);
                params.put("approved_time", String.valueOf(hour) + " : " + String.valueOf(minute) + " : " + String.valueOf(seconds) );

                JSONObject json = jsonParser.makeHttpRequest(AppData.ULRUpdateRequests, "GET", params);
                if (json != null) {
                    return json;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
            return null;
        }

        protected void onPostExecute(JSONObject json) {

            if (pDialog != null && pDialog.isShowing()) {
                pDialog.dismiss();
            }

            if(json != null) {
                try {
                    if (json.getBoolean("result")) {
                        Toast.makeText(ctxt, "Request updated.", Toast.LENGTH_LONG).show();
                    } else {
                        Toast.makeText(ctxt, "Update Failed.", Toast.LENGTH_LONG).show();
                    }
                } catch (JSONException e) {
                }
            }
        }
    }
}