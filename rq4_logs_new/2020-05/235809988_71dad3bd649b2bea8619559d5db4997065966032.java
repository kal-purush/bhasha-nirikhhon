package com.group4sweng.scranplan.Social.Messenger;

import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.TextView;

import androidx.annotation.NonNull;
import androidx.cardview.widget.CardView;
import androidx.recyclerview.widget.RecyclerView;

import com.bumptech.glide.Glide;
import com.bumptech.glide.request.RequestOptions;
import com.google.android.gms.tasks.OnCompleteListener;
import com.google.android.gms.tasks.Task;
import com.google.firebase.firestore.DocumentSnapshot;
import com.google.firebase.firestore.FirebaseFirestore;
import com.group4sweng.scranplan.R;
import com.group4sweng.scranplan.UserInfo.UserInfoPrivate;

import java.util.HashMap;
import java.util.List;

public class MessageMenuRecyclerAdapter extends RecyclerView.Adapter<MessageMenuRecyclerAdapter.ViewHolder> {
    MessengerMenu mMessengerMenu;
    List<MessageMenuRecyclerAdapter.MessengerFeedPreviewData> mDataset;
    UserInfoPrivate mUser;
    View view;

    /**  Firebase **/
    FirebaseFirestore mDatabase = FirebaseFirestore.getInstance();


    public MessageMenuRecyclerAdapter(MessengerMenu messengerMenu, List<MessageMenuRecyclerAdapter.MessengerFeedPreviewData> dataset, UserInfoPrivate mUser, View view){
        mMessengerMenu = messengerMenu;
        mDataset = dataset;
        this.mUser = mUser;
        this.view = view;
    }

    /**
     * The holder for the card with variables required
     */
    public static class MessengerFeedPreviewData {
        private String postID;
        private String authorUID;
        private String body;
        private boolean isPic;
        private String uploadedImageURL;
        private boolean isRecipe;
        private String recipeID;
        private String recipeImageURL;
        private String recipeTitle;
        private String recipeDescription;
        private boolean isReview;
        private float review;
        private HashMap<String, Object> document;

        public MessengerFeedPreviewData(HashMap<String, Object> doc) {
            this.document = doc;
            this.authorUID = document.get("UID").toString();
        }
    }

        /**
         * Building the card and image view
         */
        public static class ViewHolder extends RecyclerView.ViewHolder {
            private String authorName;
            private String authorPicURL;

            private CardView cardView;
            private TextView author;
            private ImageView authorPic;

            private ViewHolder(View v) {
                super(v);
                authorPic = v.findViewById(R.id.postAuthorPic);
                cardView = v.findViewById(R.id.messageMenuCardView);
                author = v.findViewById(R.id.postAuthor);
            }
        }

    /**
     * Building and inflating the view within its parent
     * @param parent
     * @param viewType
     * @return
     */
    @Override
    public MessageMenuRecyclerAdapter.ViewHolder onCreateViewHolder(ViewGroup parent, int viewType) {
        View v = LayoutInflater.from(parent.getContext())
                .inflate(R.layout.messenger_menu_recycler, parent, false);
        return new MessageMenuRecyclerAdapter.ViewHolder(v);
    }


    /**
     * Getting the image with picasso and adding the on click functionality
     * @param holder
     * @param position
     */
    @Override
    public void onBindViewHolder(final MessageMenuRecyclerAdapter.ViewHolder holder, int position) {
        if (mDataset.get(position).authorUID != null) {
            mDatabase.collection("users").document(mDataset.get(position).authorUID).get().addOnCompleteListener(new OnCompleteListener<DocumentSnapshot>() {
                @Override
                public void onComplete(@NonNull Task<DocumentSnapshot> task) {
                    if (task.isSuccessful()) {
                        holder.authorName = (String) task.getResult().get("displayName");
                        holder.authorPicURL = (String) task.getResult().get("imageURL");
                        holder.author.setText((String) task.getResult().get("displayName"));
                        if (task.getResult().get("imageURL") != null) {
                            Glide.with(holder.authorPic.getContext())
                                    .load(task.getResult().get("imageURL"))
                                    .apply(RequestOptions.circleCropTransform())
                                    .into(holder.authorPic);
                            holder.authorPic.setVisibility(View.VISIBLE);
                        }

                    } else {
                        Log.e("FdRc", "User details retrieval : Unable to retrieve user document in Firestore ");
                        holder.author.setText("");
                    }
                }
            });
        } else {
            Log.e("FdRc", "User UID null");
            holder.author.setText("");
        }


        holder.cardView.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                mMessengerMenu.openChat(mDataset.get(position).document.get("UID").toString());
            }
        });
    }

    // Getting dataset size
    @Override
    public int getItemCount() {
        return mDataset.size();
    }
}