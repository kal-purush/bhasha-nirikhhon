package group6.tcss450.uw.edu.chatapp;

import android.content.Context;
import android.net.Uri;
import android.os.Bundle;
import android.support.v4.app.Fragment;
import android.util.Log;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;

import org.json.JSONException;
import org.json.JSONObject;

import group6.tcss450.uw.edu.chatapp.utils.Credentials;
import group6.tcss450.uw.edu.chatapp.utils.SendPostAsyncTask;


/**
 * A simple {@link Fragment} subclass.
 * Activities that contain this fragment must implement the
 * {@link LoginFragment.OnFragmentInteractionListener} interface
 * to handle interaction events.
 */
public class LoginFragment extends Fragment {

    private OnFragmentInteractionListener mListener;
    private Credentials mCredentials;

    public LoginFragment() {
        // Required empty public constructor
    }

    @Override
    public void onAttach(Context context) {
        super.onAttach(context);
        if (context instanceof OnFragmentInteractionListener) {
            mListener = (OnFragmentInteractionListener) context;
        } else {
            throw new RuntimeException(context.toString()
                    + " must implement OnFragmentInteractionListener");
        }
    }

    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container,
                             Bundle savedInstanceState) {
        // Inflate the layout for this fragment
        View view = inflater.inflate(R.layout.fragment_login, container, false);

        Button loginButton = (Button) view.findViewById(R.id.button_loginfragment_login);
        loginButton.setOnClickListener(v -> attemptLogin());

        Button registerButton = (Button) view.findViewById(R.id.button_loginfragment_register);
        registerButton.setOnClickListener(v -> mListener.onRegisterClicked());

        return view;
    }

    private void attemptLogin() {
        EditText emailEdit = getActivity().findViewById(R.id.edittext_loginfragment_email);
        EditText passwordEdit = getActivity().findViewById(R.id.edittext_loginFragment_password);
        boolean hasError = false;

        if (emailEdit.getText().length() == 0) {
            hasError = true;
            emailEdit.setError("Email must not be empty.");
        } else if (emailEdit.getText().toString().chars().filter(ch -> ch == '@').count() != 1) {
            hasError = true;
            emailEdit.setError("Email must be valid.");
        }
        if (passwordEdit.getText().length() == 0) {
            hasError = true;
            passwordEdit.setError("Password must not be empty.");
        }

        /*
         * TEST MARKER
         */
        boolean isATest = false;

        // TODO CHANGE THIS LATER!!!
        if (!hasError) {
            if (isATest) {
                Credentials creds = new Credentials.Builder(emailEdit.getText().toString(),
                        passwordEdit.getText().toString())
                        .addUsername("Test UserName")
                        .addFirstName("First name")
                        .addLastName("Last name")
                        .build();
                mListener.onLoginSuccess(creds);
            } else {
                Credentials credentials = new Credentials.Builder(
                        emailEdit.getText().toString(),
                        passwordEdit.getText().toString())
                        .build();

                Uri uri = new Uri.Builder()
                        .scheme("https")
                        .appendPath(getString(R.string.ep_base_url))
                        .appendPath(getString(R.string.ep_login))
                        .build();

                JSONObject msg = credentials.asJSONObject();

                mCredentials = credentials;

                new SendPostAsyncTask.Builder(uri.toString(), msg)
                        .onPostExecute(this::handleLoginOnPost)
                        .onCancelled(this::handleErrorsInTask)
                        .build().execute();
            }
        }
    }

    private void handleErrorsInTask(String result) {
        Log.e("ASYNCT_TASK_ERROR", result);
    }

    private void handleLoginOnPost(String result) {
        try {
            Log.d("JSON result", result);
            JSONObject resultsJSON = new JSONObject(result);
            boolean success= resultsJSON.getBoolean("Success");

            if (success) {
                mListener.onLoginSuccess(mCredentials);
            } else {
                ((TextView) getView().findViewById(R.id.edittext_loginfragment_email))
                        .setError("Log In unsuccessful.");
            }
        } catch (JSONException e) {
            Log.e("JSON_PARSE_ERROR", result + System.lineSeparator() + e.getMessage());
            ((TextView) getView().findViewById(R.id.edittext_loginfragment_email))
                    .setError("Log In unsuccessful.");
        }
    }

    /**
     * This interface must be implemented by activities that contain this
     * fragment to allow an interaction in this fragment to be communicated
     * to the activity and potentially other fragments contained in that
     * activity.
     * <p>
     * See the Android Training lesson <a href=
     * "http://developer.android.com/training/basics/fragments/communicating.html"
     * >Communicating with Other Fragments</a> for more information.
     */
    public interface OnFragmentInteractionListener {
        void onLoginSuccess(Credentials credentials);
        void onRegisterClicked();
    }
}