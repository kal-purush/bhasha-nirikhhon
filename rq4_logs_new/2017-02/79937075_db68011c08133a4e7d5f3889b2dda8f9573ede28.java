package org.naturenet.util;


import android.content.Context;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.TextView;

import com.google.common.base.Strings;
import com.google.firebase.database.DataSnapshot;
import com.google.firebase.database.DatabaseError;
import com.google.firebase.database.FirebaseDatabase;
import com.google.firebase.database.ValueEventListener;
import com.squareup.picasso.Picasso;
import com.squareup.picasso.Transformation;

import org.naturenet.R;
import org.naturenet.data.model.Site;
import org.naturenet.data.model.Users;

import timber.log.Timber;

public class NatureNetUtils {

    private static final Transformation mAvatarTransform = new CroppedCircleTransformation();

    public static void makeUserBadge(final Context context, final ViewGroup root, final String userId) {

        View badge = View.inflate(context, R.layout.badge_horizontal, root);
        final ImageView avatar = (ImageView) badge.findViewById(R.id.badge_image);
        final TextView name = (TextView) badge.findViewById(R.id.badge_title);
        final TextView affiliation = (TextView) badge.findViewById(R.id.badge_subtitle);

        FirebaseDatabase.getInstance().getReference(Users.NODE_NAME).child(userId)
                .addListenerForSingleValueEvent(new ValueEventListener() {

            @Override
            public void onDataChange(DataSnapshot dataSnapshot) {
                Users user = dataSnapshot.getValue(Users.class);
                name.setText(user.displayName);
                Picasso.with(context).load(Strings.emptyToNull(user.avatar)).transform(mAvatarTransform)
                        .placeholder(R.drawable.default_avatar).error(R.drawable.default_avatar).fit().into(avatar);

                FirebaseDatabase.getInstance().getReference(Site.NODE_NAME).child(user.affiliation)
                        .addListenerForSingleValueEvent(new ValueEventListener() {
                    @Override
                    public void onDataChange(DataSnapshot dataSnapshot) {
                        Site site = dataSnapshot.getValue(Site.class);
                        affiliation.setText(site.name);
                    }

                    @Override
                    public void onCancelled(DatabaseError databaseError) {
                        Timber.w(databaseError.toException(), "Unable to read data for user %s", userId);
                    }
                });
            }

            @Override
            public void onCancelled(DatabaseError databaseError) {
                Timber.w(databaseError.toException(), "Unable to read data for user %s", userId);
            }
        });
    }
}