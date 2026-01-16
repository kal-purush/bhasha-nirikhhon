package kr.ac.kpu.block.smared;

import com.google.firebase.auth.FirebaseAuth;
import com.google.firebase.auth.FirebaseUser;

import java.util.function.Consumer;

public class UserAuthDAO {
    private FormattedLogger logger = new FormattedLogger();

    private Consumer<Void> successCallback;
    private Consumer<Void> failureCallback;

    public void setSuccessCallback(Consumer<Void> successCallback) {
        this.successCallback = successCallback;
    }

    public void setFailureCallback(Consumer<Void> failureCallback) {
        this.failureCallback = failureCallback;
    }

    public void signIn(String email, String password) {
        try {
            FirebaseAuth.getInstance().signInWithEmailAndPassword(email, password).addOnCompleteListener(task -> {
                if (task.isSuccessful()) {
                    logger.writeLog("Success");
                    successCallback.accept(null);
                } else {
                    logger.writeLog("Failure");
                    failureCallback.accept(null);
                }
            });
        } catch (Exception ex) {
            failureCallback.accept(null);
        }
    }

    public void signUp(String email, String password) {
        try {
            FirebaseAuth.getInstance().createUserWithEmailAndPassword(email, password).addOnCompleteListener(task -> {
                if (task.isSuccessful()) {
                    logger.writeLog("Success");
                    successCallback.accept(null);
                } else {
                    logger.writeLog("Failure");
                    failureCallback.accept(null);
                }
            });
        } catch (Exception ex) {
            failureCallback.accept(null);
        }
    }

    public void signOut() {
        try {
            logger.writeLog("Success");
            FirebaseAuth.getInstance().signOut();
            successCallback.accept(null);
        } catch (Exception ex) {
            logger.writeLog("Failure");
            failureCallback.accept(null);
        }
    }

    public void removeCurrentAccount() {
        try {
            FirebaseUser currentUser = FirebaseAuth.getInstance().getCurrentUser();
            signOut();

            currentUser.delete().addOnCompleteListener((task) -> {
                if (task.isSuccessful()) {
                    logger.writeLog("Success");
                    successCallback.accept(null);
                } else {
                    logger.writeLog("Failure");
                    failureCallback.accept(null);
                }
            });
        } catch (Exception ex) {
            logger.writeLog("Failure");
            failureCallback.accept(null);
        }
    }
}