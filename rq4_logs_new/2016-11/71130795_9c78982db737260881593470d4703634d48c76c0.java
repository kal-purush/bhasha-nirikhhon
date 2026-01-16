package com.example.danil.skilder;

import android.content.ActivityNotFoundException;
import android.content.Intent;
import android.graphics.Bitmap;
import android.net.Uri;
import android.os.Bundle;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.Toast;

import com.facebook.CallbackManager;
import com.facebook.FacebookCallback;
import com.facebook.FacebookException;
import com.facebook.share.Sharer;
import com.facebook.share.model.ShareLinkContent;
import com.facebook.share.model.SharePhoto;
import com.facebook.share.model.SharePhotoContent;
import com.facebook.share.widget.ShareDialog;
import com.google.android.gms.plus.PlusShare;


public class ShareFragment extends BaseFragment{

    public static final String POST_GOOGLE = "ShareFragment_POST_GOOGLE";
    public static final String POST_VK = "ShareFragment_POST_VK";
    public static final String POST_FACEBOOK = "ShareFragment_POST_FACEBOOK";

    private static final int GOOGLEPLUS_REQUEST_CODE = 1001;

    Button mButtonGoogle;
    Button mButtonVK;
    Button mButtonFacebook;

    public ShareFragment() {}

    @Override
    public void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
    }

    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container,
                             Bundle savedInstanceState) {
        return inflater.inflate(R.layout.fragment_share, container, false);
    }

    @Override
    public void onActivityCreated(Bundle savedInstanceState) {
        super.onActivityCreated(savedInstanceState);

        DrawTool tool = DrawTool.getInstance();

        mButtonGoogle = (Button) getView().findViewById(R.id.share_google);
        mButtonVK = (Button) getView().findViewById(R.id.share_vk);
        mButtonFacebook = (Button) getView().findViewById(R.id.share_instagramm);

        mButtonGoogle.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                postGoogle();
                onFragmentInteraction(POST_GOOGLE, null);
            }
        });

        mButtonVK.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                onFragmentInteraction(POST_VK, null);
            }
        });

        mButtonFacebook.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                postFacebook();
                onFragmentInteraction(POST_FACEBOOK, null);
            }
        });
    }

    private void postGoogle() {
        try {
            Intent intent = new PlusShare.Builder(this.getActivity())
                    .setType("text/plain")
                    .setText("Welcome to the Google+ platform.")
                    .setContentUrl(Uri.parse("https://developers.google.com/+/"))
                    .getIntent();

            startActivityForResult(intent, 0);
        }
        catch (ActivityNotFoundException ex) {
            Toast.makeText(this.getActivity(), "Google+ is missing", Toast.LENGTH_SHORT).show();
        }
    }

    private void postVK() {

    }

    private void postFacebook() {
        ShareDialog shareDialog = new ShareDialog(this);
        CallbackManager callbackManager = CallbackManager.Factory.create();
        shareDialog.registerCallback(callbackManager, new FacebookCallback<Sharer.Result>() {
            @Override
            public void onSuccess(Sharer.Result result) {

            }

            @Override
            public void onCancel() {

            }

            @Override
            public void onError(FacebookException error) {

            }
        });

        if (ShareDialog.canShow(SharePhotoContent.class)) {
            Bitmap image = DrawStateManager.getInstance().getCurrentScreen();
            SharePhoto photo = new SharePhoto.Builder()
                    .setBitmap(image)
                    .build();
            SharePhotoContent content = new SharePhotoContent.Builder()
                    .addPhoto(photo)
                    .build();

            shareDialog.show(content);
            Toast.makeText(this.getActivity(), "Posted to facebook", Toast.LENGTH_SHORT).show();
        }
        else {
            Toast.makeText(this.getActivity(), "Unable to post on facebook (facebook app should be installed >= 7.0)", Toast.LENGTH_SHORT).show();
        }

    }

    public void onActivityResult(int requestCode, int resultCode, Intent data) {
        if ((requestCode == GOOGLEPLUS_REQUEST_CODE) && (resultCode == -1)) {
            //Do something if success
        }
    }

}