package com.example.tanvi.ireport.Activity;

import android.app.ProgressDialog;
import android.content.Intent;
import android.support.annotation.NonNull;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.util.Log;
import android.view.View;
import android.widget.Toast;

import com.example.tanvi.ireport.R;
import com.facebook.AccessToken;
import com.facebook.CallbackManager;
import com.facebook.FacebookCallback;
import com.facebook.FacebookException;
import com.facebook.FacebookSdk;
import com.facebook.login.LoginResult;
import com.facebook.login.widget.LoginButton;
import com.google.android.gms.auth.api.Auth;
import com.google.android.gms.auth.api.signin.GoogleSignInAccount;
import com.google.android.gms.auth.api.signin.GoogleSignInOptions;
import com.google.android.gms.auth.api.signin.GoogleSignInResult;
import com.google.android.gms.common.ConnectionResult;
import com.google.android.gms.common.SignInButton;
import com.google.android.gms.common.api.GoogleApiClient;
import com.google.android.gms.tasks.OnCompleteListener;
import com.google.android.gms.tasks.Task;
import com.google.firebase.auth.AuthCredential;
import com.google.firebase.auth.AuthResult;
import com.google.firebase.auth.FacebookAuthProvider;
import com.google.firebase.auth.FirebaseAuth;
import com.google.firebase.auth.FirebaseUser;
import com.google.firebase.auth.GoogleAuthProvider;

public class LoginActivity extends AppCompatActivity implements GoogleApiClient.OnConnectionFailedListener{


        SignInButton googleSignIn;
        LoginButton facebookSignIn;
        ProgressDialog progressDialog;
        GoogleApiClient mGoogleApiClient;
        private FirebaseAuth firebaseAuth;
        private CallbackManager callbackManager;
        private FirebaseAuth.AuthStateListener mAuthStateListener;
        private static final int RC_SIGN_IN = 9001;
        private static final String TAG = LoginActivity.class.getName();

        @Override
        protected void onCreate(Bundle savedInstanceState) {
            super.onCreate(savedInstanceState);

            /************Initialize Facebook SDK ***********/
            FacebookSdk.sdkInitialize(getApplicationContext());

            /****************Set the view as login page**********/
            setContentView(R.layout.activity_login);

            /**************Process Dialog*************/
            progressDialog = new ProgressDialog(this);

            /***************Get Instance of FirebaseAuth and set a state listener*************/
            firebaseAuth=FirebaseAuth.getInstance();
            mAuthStateListener = new FirebaseAuth.AuthStateListener(){

                @Override
                public void onAuthStateChanged(@NonNull FirebaseAuth firebaseAuth) {
                    FirebaseUser user = firebaseAuth.getCurrentUser();
                    if(user!=null){
                        Log.d(TAG,"OnAuthStateChanged:SignedIn"+user.getUid());
                    }else {
                        Log.d(TAG,"OnAuthStateChanged:SignedOut");
                    }
                }
            };


            /************************Sign In using Google***********************/
            googleSignIn = (SignInButton) findViewById(R.id.google_login_button);
            googleSignIn.setOnClickListener(new View.OnClickListener(){

                @Override
                public void onClick(View view) {
                    setGoogleSignIn();
                }
            });

            GoogleSignInOptions googleSignInOptions = new GoogleSignInOptions.Builder(GoogleSignInOptions.DEFAULT_SIGN_IN).requestIdToken(getString(R.string.default_web_client_id)).requestEmail().build();

            mGoogleApiClient = new GoogleApiClient.Builder(this)
                    .enableAutoManage(this /* LoginActivity */, this /* OnConnectionFailedListener */)
                    .addApi(Auth.GOOGLE_SIGN_IN_API, googleSignInOptions)
                    .build();



            /************************Sign In using Facebook***********************/

            /**********Initialize instance of Callback Manager***********/
            callbackManager = CallbackManager.Factory.create();

            facebookSignIn = (LoginButton) findViewById(R.id.facebook_login_button);

            /********Set read permissions for only email*******************/
            facebookSignIn.setReadPermissions("email");

            /******Register a callback********/
            facebookSignIn.registerCallback(callbackManager, new FacebookCallback<LoginResult>() {
                @Override
                public void onSuccess(LoginResult loginResult) {
                    Log.d(TAG,"Facebook: onSuccess: "+loginResult);
                    firebaseAuthWithFacebook(loginResult.getAccessToken());
                }

                @Override
                public void onCancel() {
                    Log.d(TAG,"Facebook: onCancel");
                }

                @Override
                public void onError(FacebookException error) {
                    Log.d(TAG,"Facebook: onError" +error);
                }
            });




        }


    /************Firebase Authentication using Facebook****************/
    private void firebaseAuthWithFacebook(AccessToken accessToken) {

        Log.d(TAG,"Facebook Access Token :" +accessToken);

        AuthCredential credential = FacebookAuthProvider.getCredential(accessToken.getToken());
        firebaseAuth.signInWithCredential(credential)
                .addOnCompleteListener(this, new OnCompleteListener<AuthResult>() {
                    @Override
                    public void onComplete(@NonNull Task<AuthResult> task) {
                        if(!task.isSuccessful()){

                            Toast.makeText(LoginActivity.this, "Facebook Authentication Failed", Toast.LENGTH_SHORT).show();
                        }
                        else
                        {
                            Toast.makeText(LoginActivity.this, "Facebook Authentication Successful", Toast.LENGTH_SHORT).show();
                            //Intent intent = new Intent(LoginActivity.this,MyReportActivity.class);
                            //startActivity(intent);

                        }
                    }
                });


    }


    /********Set the GoogleAPIClient and start the Activity*************/
    private void setGoogleSignIn()
    {
        Intent signInIntent = Auth.GoogleSignInApi.getSignInIntent(mGoogleApiClient);
        startActivityForResult(signInIntent,RC_SIGN_IN);

    }

    @Override
    protected void onActivityResult(int requestCode, int resultCode, Intent data) {
        super.onActivityResult(requestCode, resultCode, data);

        /*****Google********/
        if(requestCode == RC_SIGN_IN){
            GoogleSignInResult googleSignInResult = Auth.GoogleSignInApi.getSignInResultFromIntent(data);
            if(googleSignInResult.isSuccess()){
                GoogleSignInAccount googleSignInAccount = googleSignInResult.getSignInAccount();
                firebaseAuthWithGoogle(googleSignInAccount);

            }

        }

        /********Facebook******/
        else
        {
            callbackManager.onActivityResult(requestCode,resultCode,data);
        }
    }


    /************Firebase Authentication using Facebook****************/
    private void firebaseAuthWithGoogle(GoogleSignInAccount account){
        Log.d(TAG,"Firebase Auth With Google:"+account.getId());
        AuthCredential credential = GoogleAuthProvider.getCredential(account.getIdToken(),null);
        firebaseAuth.signInWithCredential(credential).addOnCompleteListener(this, new OnCompleteListener<AuthResult>() {
            @Override
            public void onComplete(@NonNull Task<AuthResult> task) {


                if(!task.isSuccessful()){
                    Log.w(TAG,"signInWithCredential",task.getException());
                    Toast.makeText(LoginActivity.this,"Google Authentication Failed",Toast.LENGTH_SHORT).show();
                }

                else
                    Toast.makeText(LoginActivity.this,"Google Authentication Success",Toast.LENGTH_SHORT).show();




            }
        });

    }


    @Override
    public void onConnectionFailed(@NonNull ConnectionResult connectionResult) {

        Log.d(TAG, "Connection Failed: "+connectionResult);
    }

    @Override
    protected void onStart() {
        super.onStart();
        firebaseAuth.addAuthStateListener(mAuthStateListener);
    }

    @Override
    protected void onStop() {
        super.onStop();
        if(mAuthStateListener!=null){
            firebaseAuth.removeAuthStateListener(mAuthStateListener);
        }
    }
}