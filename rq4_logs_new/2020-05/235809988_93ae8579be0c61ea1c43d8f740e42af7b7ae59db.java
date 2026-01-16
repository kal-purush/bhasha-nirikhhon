package com.group4sweng.scranplan.Social;

import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.ImageView;
import android.widget.LinearLayout;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.cardview.widget.CardView;
import androidx.recyclerview.widget.RecyclerView;

import com.bumptech.glide.Glide;
import com.bumptech.glide.request.RequestOptions;
import com.google.android.gms.tasks.OnCompleteListener;
import com.google.android.gms.tasks.Task;
import com.google.firebase.Timestamp;
import com.google.firebase.firestore.DocumentSnapshot;
import com.google.firebase.firestore.FirebaseFirestore;
import com.group4sweng.scranplan.R;
import com.group4sweng.scranplan.UserInfo.UserInfoPrivate;

import java.util.List;

/**
 *  Class holding the recycler adapter for the search functionality, each card will represent the view
 *  of one recipe. All recipe info is stored in this card.
 *  Creating a card view that hold the picture and the document which, the picture will be displayed
 *  in a button and the button will pass the document though for the recipe to be read
 */
public class NotificationRecyclerAdapter extends RecyclerView.Adapter<NotificationRecyclerAdapter.ViewHolder> {

    /**  Firebase **/
    FirebaseFirestore mDatabase = FirebaseFirestore.getInstance();

    // Variables for database and fragment to be displayed in
    private UserInfoPrivate user;

    private List<NotificationData> mDataset;
    private String currentSlide;
    private String postID;

    public static class NotificationData {
        private String senderID;
        private String body;
        private DocumentSnapshot document;
        private String timestamp;
        private String relatedPostID;
        private boolean ifRequest;

        /**
         * The holder for the card with variables required
         */
        public NotificationData(boolean ifRequest, DocumentSnapshot doc, String senderID, String body, Timestamp timestamp, String postID) {
            this.document = doc;
            this.senderID = senderID;
            this.body = body;
            if(timestamp != null){
                this.timestamp = timestamp.toDate().toString();
            }
            this.ifRequest = ifRequest;
            if(ifRequest){
                this.relatedPostID = postID;
            }
        }
    }

    /**
     * Building the card and image view
     */
    public static class ViewHolder extends RecyclerView.ViewHolder {

        private CardView cardView;
        private TextView senderName;
        private TextView body;
        private TextView timestamp;
        private ImageView senderPic;
        private boolean followRequest;
        private Button accept;
        private Button reject;
        private LinearLayout buttonsLayout;
        private LinearLayout timestampLayout;



        private ViewHolder(View v) {
            super(v);
            cardView = v.findViewById(R.id.notificationListCardView);
            senderName = v.findViewById(R.id.sender);
            body = v.findViewById(R.id.notificationMessage);
            timestamp = v.findViewById(R.id.notificationTimeStamp);
            senderPic = v.findViewById(R.id.senderPic);
            accept = v.findViewById(R.id.acceptButton);
            reject = v.findViewById(R.id.rejectButton);
            buttonsLayout = v.findViewById(R.id.buttonsLayout);
            timestampLayout = v.findViewById(R.id.timestampLayout);
        }
    }

    /**
     * Constructor to add all variables
     * @param dataset
     */
    public NotificationRecyclerAdapter(List<NotificationData> dataset, UserInfoPrivate user) {
        mDataset = dataset;
        this.user = user;
    }



    /**
     * Building and inflating the view within its parent
     * @param parent
     * @param viewType
     * @return
     */
    public NotificationRecyclerAdapter.ViewHolder onCreateViewHolder(ViewGroup parent, int viewType) {
        View v = LayoutInflater.from(parent.getContext())
                .inflate(R.layout.notification, parent, false);
        return new NotificationRecyclerAdapter.ViewHolder(v);
    }

    /**
     * Getting the image with picasso and adding the on click functionality
     * @param holder
     * @param position
     */
    @Override
    public void onBindViewHolder(final NotificationRecyclerAdapter.ViewHolder holder, int position) {
        holder.body.setText(mDataset.get(position).body);
        if(mDataset.get(position).senderID != null){
            mDatabase.collection("users").document(mDataset.get(position).senderID).get().addOnCompleteListener(new OnCompleteListener<DocumentSnapshot>() {
                @Override
                public void onComplete(@NonNull Task<DocumentSnapshot> task) {
                    if(task.isSuccessful()){
                        DocumentSnapshot doc = task.getResult();
                        if(doc != null){
                            holder.senderName.setText((String)doc.get("displayName"));
                            if(doc.get("imageURL") != null){
                                Glide.with(holder.senderPic.getContext())
                                        .load(doc.get("imageURL"))
                                        .apply(RequestOptions.circleCropTransform())
                                        .into(holder.senderPic);
                            }
                        }
                    }
                }
            });
        }

        if(mDataset.get(position).ifRequest){
            holder.buttonsLayout.setVisibility(View.VISIBLE);

        }
        if(mDataset.get(position).timestamp != null){
            holder.timestampLayout.setVisibility(View.VISIBLE);
            holder.timestamp.setText(mDataset.get(position).timestamp);
        }




    }

    // Getting dataset size
    @Override
    public int getItemCount() {
        return mDataset.size();
    }
}