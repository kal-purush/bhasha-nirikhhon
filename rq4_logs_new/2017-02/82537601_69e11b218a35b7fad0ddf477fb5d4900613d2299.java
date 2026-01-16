package com.example.phuong.viectimnguoiapp.activities;

import android.app.ProgressDialog;
import android.content.SharedPreferences;
import android.view.View;
import android.view.ViewGroup;
import android.widget.EditText;
import android.widget.ImageView;
import android.widget.LinearLayout;
import android.widget.ScrollView;
import android.widget.TextView;

import com.example.phuong.viectimnguoiapp.R;
import com.example.phuong.viectimnguoiapp.utils.Constant;
import com.firebase.client.ChildEventListener;
import com.firebase.client.DataSnapshot;
import com.firebase.client.Firebase;
import com.firebase.client.FirebaseError;

import org.androidannotations.annotations.Click;
import org.androidannotations.annotations.EActivity;
import org.androidannotations.annotations.Extra;
import org.androidannotations.annotations.ViewById;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by phuong on 23/02/2017.
 */
@EActivity(R.layout.activity_contact_detail)
public class DetailContactActivity extends BaseActivity {
    @Extra
    protected String idUserContact;
    @ViewById(R.id.imgBack)
    ImageView mImgBack;
    @ViewById(R.id.scrollView)
    ScrollView mScrollView;
    @ViewById(R.id.layoutContact)
    LinearLayout mLlContact;
    @ViewById(R.id.imgSend)
    ImageView mImgSend;
    @ViewById(R.id.edtMessageArea)
    EditText mEdtMessageArea;
    private Firebase mFirebase1;
    private Firebase mFirebase2;
    private SharedPreferences mSharedPreferences;
    private String idUser;
    private ProgressDialog pd;

    @Override
    void inits() {

        pd = new ProgressDialog(this);
        pd.setMessage("Loading...");
        pd.show();

        mSharedPreferences = getSharedPreferences(Constant.DATA_NAME_USER_LOGIN, 0);
        idUser = mSharedPreferences.getString(Constant.ID_USER_LOGIN, "");
        idUser = "2";

        Firebase.setAndroidContext(this);
        mFirebase1 = new Firebase("https://viectimnguoi-469e6.firebaseio.com/messages/" + idUser + "_" + idUserContact);
        mFirebase2 = new Firebase("https://viectimnguoi-469e6.firebaseio.com/messages/" + idUserContact + "_" + idUser);


        mImgSend.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                String messageText = mEdtMessageArea.getText().toString();

                if (!messageText.equals("")) {
                    Map<String, String> map = new HashMap<String, String>();
                    map.put("message", messageText);
                    map.put("user", idUser);
                    mFirebase1.push().setValue(map);
                    mFirebase2.push().setValue(map);
                    mEdtMessageArea.setText("");
                }
            }
        });

        mFirebase1.addChildEventListener(new ChildEventListener() {
            @Override
            public void onChildAdded(DataSnapshot dataSnapshot, String s) {
                Map map = dataSnapshot.getValue(Map.class);
                String message = map.get("message").toString();
                String userName = map.get("user").toString();

                if (userName.equals(idUser)) {
                    addMessageBox("You:\n" + message, 1);
                } else {
                    addMessageBox(idUserContact + ":\n" + message, 2);
                }
                pd.dismiss();
            }

            @Override
            public void onChildChanged(DataSnapshot dataSnapshot, String s) {

            }

            @Override
            public void onChildRemoved(DataSnapshot dataSnapshot) {

            }

            @Override
            public void onChildMoved(DataSnapshot dataSnapshot, String s) {

            }

            @Override
            public void onCancelled(FirebaseError firebaseError) {

            }
        });
    }

    public void addMessageBox(String message, int type) {
        TextView textView = new TextView(DetailContactActivity.this);
        textView.setText(message);
        LinearLayout.LayoutParams lp = new LinearLayout.LayoutParams(ViewGroup.LayoutParams.WRAP_CONTENT, ViewGroup.LayoutParams.WRAP_CONTENT);


        if (type == 1) {
            lp.setMargins(100, 0, 0, 10);
            textView.setLayoutParams(lp);
            textView.setTextColor(getResources().getColor(R.color.bg_default));
            textView.setBackgroundResource(R.drawable.rounded_corner1);
        } else {
            lp.setMargins(0, 0, 0, 10);
            textView.setLayoutParams(lp);
            textView.setBackgroundResource(R.drawable.rounded_corner2);
        }

        mLlContact.addView(textView);
        mScrollView.fullScroll(View.FOCUS_DOWN);
    }

    @Click(R.id.imgBack)
    public void backAction() {
        finish();
    }
}