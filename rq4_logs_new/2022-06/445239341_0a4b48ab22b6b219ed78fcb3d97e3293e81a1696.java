package password.vault.client.gui;

import javafx.scene.control.Alert;

public class CommonUIElements {
    private static Alert getAlertMessage(Alert.AlertType type, String header, String context, String title) {
        Alert alert = new Alert(type);

        alert.setTitle(title);
        alert.setHeaderText(header);
        alert.setContentText(context);

        return alert;
    }

    public static Alert getQuitAlert() {
        return getAlertMessage(Alert.AlertType.CONFIRMATION,
                               "Quitting Password Vault",
                               "Are you sure you want to exit?",
                               "Quit confirmation");
    }

    public static Alert getInformationAlert(String text, String context) {
        return getAlertMessage(Alert.AlertType.INFORMATION, text, context, "");
    }

    public static Alert getErrorAlert(String text) {
        return getAlertMessage(Alert.AlertType.ERROR, text, "", "");
    }

    public static Alert getFailedRequestWarningAlert() {
        return getAlertMessage(Alert.AlertType.WARNING, "Couldn't complete " +
                "your request!", "", "");
    }
}