package tr.edu.msku.steprace.adapter;

import android.content.DialogInterface;
import android.content.Intent;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageButton;
import android.widget.ImageView;
import android.widget.TextView;
import androidx.annotation.NonNull;
import androidx.appcompat.app.AlertDialog;
import androidx.recyclerview.widget.RecyclerView;

import com.google.android.gms.tasks.OnSuccessListener;
import com.google.firebase.auth.FirebaseAuth;
import com.google.firebase.firestore.CollectionReference;
import com.google.firebase.firestore.DocumentReference;
import com.google.firebase.firestore.DocumentSnapshot;
import com.google.firebase.firestore.FirebaseFirestore;
import com.squareup.picasso.Picasso;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jp.wasabeef.picasso.transformations.CropCircleTransformation;
import tr.edu.msku.steprace.MainActivity;
import tr.edu.msku.steprace.R;
import tr.edu.msku.steprace.model.User;

public class NotificationsAdapter extends RecyclerView.Adapter<NotificationsAdapter.ViewHolderNotification> {

    private List<User> users = new ArrayList<>();
    private FirebaseFirestore db = FirebaseFirestore.getInstance();
    private String user_id = FirebaseAuth.getInstance().getCurrentUser().getUid().toString();
    DocumentReference ref = db.collection("Image").document(user_id);

    public NotificationsAdapter(List<User> users) {
        this.users = users;
    }


    @NonNull
    @Override
    public ViewHolderNotification onCreateViewHolder(@NonNull ViewGroup parent, int viewType) {
        View view = LayoutInflater.from(parent.getContext())
                .inflate(R.layout.notification_row, parent, false);
        return new NotificationsAdapter.ViewHolderNotification(view);
    }

    @Override
    public void onBindViewHolder(@NonNull final ViewHolderNotification holder, final int position) {
        holder.name.setText(users.get(position).getName());
        holder.surname.setText(users.get(position).getSurname());
        holder.city.setText(users.get(position).getCity());
        holder.profile_photo.setImageResource(R.drawable.user);

        ref.get().addOnSuccessListener(new OnSuccessListener<DocumentSnapshot>() {
            @Override
            public void onSuccess(DocumentSnapshot documentSnapshot) {

                String image = documentSnapshot.get("image").toString();

                if (documentSnapshot.exists()){

                    Picasso.get()
                            .load(image)
                            .transform(new CropCircleTransformation())
                            .into(holder.profile_photo);

                }
                else{

                    holder.profile_photo.setImageResource(R.drawable.user);
                }
            };


        });





        holder.imgBtn_reject.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(final View v) {
                AlertDialog.Builder alertDialog = new AlertDialog.Builder(v.getRootView().getContext());
                alertDialog.setTitle("Decline Request");
                alertDialog.setMessage("Are you sure you want to decline the Request?");

                alertDialog.setPositiveButton("Yes", new DialogInterface.OnClickListener() {
                    public void onClick(DialogInterface dialog,int which) {


                        Intent decline = new Intent(v.getContext(), MainActivity.class);
                        decline.putExtra("decline",users.get(position).getUser_id());
                        v.getContext().startActivity(decline);


                        Map declineMap = new HashMap();

                        declineMap.put("Notifications/" + "/" + users.get(position).getName() + "/" + users.get(position).getUser_id(),null);
                        declineMap.put("Notifications/" + "/" + users.get(position).getSurname() + "/" +users.get(position),null);
                        declineMap.put("Notifications/" + "/" + users.get(position).getCity()+ "/" +users.get(position),null);

                    }
                });
                alertDialog.setNegativeButton("No", new DialogInterface.OnClickListener() {
                    public void onClick(DialogInterface dialog, int which) {
                        dialog.cancel();
                    }
                });

                alertDialog.show();
            }
    });

        holder.imgBtn_accept.setOnClickListener(new View.OnClickListener() {
                @Override
                public void onClick(final View v) {
                    AlertDialog.Builder alertDialog = new AlertDialog.Builder(v.getRootView().getContext());
                    alertDialog.setTitle("Accept Request");
                    alertDialog.setMessage("Are you sure you want to accept the Request?");

                    alertDialog.setPositiveButton("Yes", new DialogInterface.OnClickListener() {
                        public void onClick(DialogInterface dialog,int which) {
                            Intent accept = new Intent(v.getContext(), MainActivity.class);
                            accept.putExtra("acceptid",users.get(position).getUser_id());
                            String accept_id = users.get(position).getUser_id();
                            v.getContext().startActivity(accept);
                        }
                    });
                    alertDialog.setNegativeButton("No", new DialogInterface.OnClickListener() {
                        public void onClick(DialogInterface dialog, int which) {
                            dialog.cancel();
                        }
                    });

                    alertDialog.show();
                }
            });


    }

    @Override
    public int getItemCount() {
        return users.size();
    }

    public static class ViewHolderNotification extends RecyclerView.ViewHolder {

        public TextView name;
        public ImageView profile_photo;
        public TextView surname;
        public TextView city;
        public ImageButton imgBtn_accept;
        public ImageButton imgBtn_reject;


        public ViewHolderNotification(@NonNull View itemView) {
            super(itemView);
            name = (TextView) itemView.findViewById(R.id.name);
            surname = (TextView) itemView.findViewById(R.id.surname);
            city = (TextView) itemView.findViewById(R.id.city);
            profile_photo = (ImageView) itemView.findViewById(R.id.image_search);
            imgBtn_reject = itemView.findViewById(R.id.imgBtn_reject);
            imgBtn_accept = itemView.findViewById(R.id.imgBtn_accept);


        }
    }
}