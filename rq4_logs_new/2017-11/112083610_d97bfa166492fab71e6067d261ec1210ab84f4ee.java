package com.tline.android.utils;

import android.content.Context;
import android.content.DialogInterface;
import android.support.v7.app.AlertDialog;
import android.view.LayoutInflater;
import android.view.View;
import android.widget.FrameLayout;
import android.widget.ImageView;
import android.widget.TextView;

import com.tline.android.R;
import com.twitter.sdk.android.core.models.Tweet;

import timber.log.Timber;

/**
 * Created by Naeem(naeemark@gmail.com)
 * On 29/11/2017.
 * For TLine
 */

public class DialogUtils {

    public static void showTweetDialog(Context context, Tweet tweet) {
        AlertDialog.Builder builder = new AlertDialog.Builder(context)
                .setTitle(tweet.user.name)
                .setCancelable(true)
                .setIcon(R.drawable.ic_launcher_round_web)
                ;

        builder.setPositiveButton(context.getString(R.string.lbl_button_close), new DialogInterface.OnClickListener() {
            @Override
            public void onClick(DialogInterface dialog, int which) {
               dialog.dismiss();
            }
        });

        final FrameLayout frameView = new FrameLayout(context);
        builder.setView(frameView);

        final AlertDialog alertDialog = builder.create();
        LayoutInflater inflater = alertDialog.getLayoutInflater();
        View view = inflater.inflate(R.layout.item_list, frameView);

        view.findViewById(R.id.tvUserName).setVisibility(View.GONE);
        view.findViewById(R.id.wrapper_footer).setVisibility(View.VISIBLE);

        TextView screenName = view.findViewById(R.id.tvTwitterHandle);
        TextView tweetText = view.findViewById(R.id.tvTweetText);
        TextView timeSent = view.findViewById(R.id.tvTimeSend);
        ImageView dpImage = view.findViewById(R.id.imageView_dp);
        ImageView  tweetImage = view.findViewById(R.id.ivTweetImage);
        TextView time = view.findViewById(R.id.replyCount);
        TextView retweets = view.findViewById(R.id.reTweetCount);
        TextView likes = view.findViewById(R.id.likeCount);

        timeSent.setText(DateTimeUtils.getRelativeTimeAgo(tweet.createdAt));
        screenName.setText(context.getString(R.string.prefix_screenname, tweet.user.screenName));

        tweetText.setText(tweet.text);

        time.setText(DateTimeUtils.formatTimeForFooter(tweet.createdAt));
        retweets.setText(String.valueOf(tweet.retweetCount));
        likes.setText(String.valueOf(tweet.favoriteCount));

        ImageUtils.loadImage(context, dpImage, tweet.user.profileImageUrl);

        if (!tweet.entities.media.isEmpty() && tweet.entities.media.get(0).type.equals("photo")) {
            ImageUtils.loadImage(context, tweetImage, tweet.entities.media.get(0).mediaUrl);
            tweetImage.setVisibility(View.VISIBLE);
        }
        Timber.e(tweet.toString());

        alertDialog.show();
    }
}