package com.vrocketz.spotchu.activity.fragment;

import java.util.Arrays;

import com.facebook.AppEventsLogger;
import com.facebook.Session;
import com.facebook.SessionState;
import com.facebook.UiLifecycleHelper;
import com.facebook.widget.LoginButton;
import com.vrocketz.spotchu.R;
import com.vrocketz.spotchu.helper.Config;
import com.vrocketz.spotchu.helper.Constants;

import android.content.Intent;
import android.os.Bundle;
import android.support.v4.app.Fragment;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;

public class FaceBookFragment extends Fragment {

	private UiLifecycleHelper uiHelper;

	@Override
	public void onCreate(Bundle savedInstanceState) {
		// TODO Auto-generated method stub
		super.onCreate(savedInstanceState);

		// Initialize facebook login UI Helper

		uiHelper = new UiLifecycleHelper(getActivity(), callback);
		uiHelper.onCreate(savedInstanceState);

		// Initialize FB login button LoginButton authButton =
		// (LoginButton) findViewById(R.id.authButton);
		// authButton.setReadPermissions(Arrays.asList("public_profile", "email"));

	}
	
	@Override
	public View onCreateView(LayoutInflater inflater, 
	        ViewGroup container, 
	        Bundle savedInstanceState) {
	    View view = inflater.inflate(R.layout.fbbutton, container, false);
	    
	    LoginButton authButton = (LoginButton) view.findViewById(R.id.authButton);
	    authButton.setFragment(this);
	    authButton.setReadPermissions(Arrays.asList("public_profile", "email"));

	    return view;

	}
	
	@Override
	public void onResume() {
	    super.onResume();
	    Session session = Session.getActiveSession();
		if (session != null && (session.isOpened() || session.isClosed())) {
			if (Config.DEBUG)
				Log.d(Constants.APP_NAME, "FB Session is present.");
			onSessionStateChange(session, session.getState(), null);
		}

	    uiHelper.onResume();
	    AppEventsLogger.activateApp(getActivity());
	}

	@Override
	public void onActivityResult(int requestCode, int resultCode, Intent data) {
	    super.onActivityResult(requestCode, resultCode, data);
	    uiHelper.onActivityResult(requestCode, resultCode, data);
	}
	
	@Override
	public void onPause() {
	    super.onPause();
	    uiHelper.onPause();
	 // Logs 'app deactivate' App Event.
	 	AppEventsLogger.deactivateApp(getActivity());
	}

	@Override
	public void onDestroy() {
	    super.onDestroy();
	    uiHelper.onDestroy();
	}

	@Override
	public void onSaveInstanceState(Bundle outState) {
	    super.onSaveInstanceState(outState);
	    uiHelper.onSaveInstanceState(outState);
	}

	private void onSessionStateChange(Session session, SessionState state,
			Exception exception) {
		if (state.isOpened()) {
			Log.d(Constants.APP_NAME, "Logged in...");
			// TODO : register user to spotchu.
			// startMainActivity();
		} else if (state.isClosed()) {
			Log.d(Constants.APP_NAME, "Logged out...");
		} else {
			Log.d(Constants.APP_NAME, state.name());
		}
	}

	private Session.StatusCallback callback = new Session.StatusCallback() {
		@Override
		public void call(Session session, SessionState state,
				Exception exception) {
			onSessionStateChange(session, state, exception);
		}
	};
}