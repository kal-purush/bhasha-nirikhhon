package com.example.oguz.whatsaround;

import android.content.Intent;
import android.net.Uri;
import android.os.Bundle;
import android.support.annotation.Nullable;
import android.support.v4.app.Fragment;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.TextView;
import android.widget.Toast;

import com.google.firebase.database.DataSnapshot;
import com.google.firebase.database.DatabaseError;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;
import com.google.firebase.database.ValueEventListener;

/**
 * Created by oguz on 25.05.2017.
 */

public class UserCompanyFragment extends Fragment {
    DatabaseReference mDatabase;
    public View onCreateView(LayoutInflater inflater, @Nullable ViewGroup container, @Nullable Bundle savedInstanceState) {
        View v = inflater.inflate(R.layout.fragment_user_company,container,false);
        final String uid = getArguments().getString("uid");

        if(uid==null) {
            Toast.makeText(getContext(),"Something went wrong!",Toast.LENGTH_SHORT).show();
            return null;
        }

        final TextView tvName = (TextView) v.findViewById(R.id.txtFirmaName);
        final TextView tvPhone = (TextView) v.findViewById(R.id.tvPhone);
        final Button btnCall = (Button) v.findViewById(R.id.btnCall);
        final Button btnSms = (Button) v.findViewById(R.id.btnSms);
        final Button btnDirections = (Button) v.findViewById(R.id.btnDirections);


        mDatabase = FirebaseDatabase.getInstance().getReference();
        mDatabase.addListenerForSingleValueEvent(new ValueEventListener() {
            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
                if(dataSnapshot.child("companies").child(uid).exists()){
                    String name = dataSnapshot.child("companies").child(uid).child("name").getValue().toString();
                    String email = dataSnapshot.child("companies").child(uid).child("email").getValue().toString();
                    final String phone = dataSnapshot.child("companies").child(uid).child("phone").getValue().toString();
                    String service_id = dataSnapshot.child("companies").child(uid).child("serviceId").getValue().toString();

                    String latlng = dataSnapshot.child("company_list").child(service_id).child(uid).getValue().toString();

                    tvName.setText(name.replaceAll("@"," "));
                    tvPhone.setText(phone);

                    String strLat = latlng.split("-")[0];

                    Double lat = Double.valueOf(latlng.split("-")[0]);
                    Double lng = Double.valueOf(latlng.split("-")[1]);

                    Bundle bundle = new Bundle();
                    bundle.putDouble("lat",lat);
                    bundle.putDouble("lng",lng);
                    bundle.putString("name",name);

                    Fragment fr = new MapFragment();
                    fr.setArguments(bundle);
                    getActivity().getSupportFragmentManager().beginTransaction()
                            .add(R.id.map_container,fr).commit();

                    btnCall.setOnClickListener(new View.OnClickListener() {
                        @Override
                        public void onClick(View v) {
                            Intent callIntent = new Intent(Intent.ACTION_CALL);
                            callIntent.setData(Uri.parse("tel:"+phone));
                            startActivity(callIntent);
                        }
                    });
                    btnSms.setOnClickListener(new View.OnClickListener() {
                        @Override
                        public void onClick(View v) {
                            Intent intent = new Intent(Intent.ACTION_VIEW, Uri.parse("sms:" + phone));
                            startActivity(intent);
                        }
                    });
                }
            }
            @Override
            public void onCancelled(DatabaseError databaseError) {

            }
        });



        return v;
    }
}