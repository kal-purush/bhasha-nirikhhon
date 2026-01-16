/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package controller.scene_controller.sign_up_scenes;

import com.jfoenix.controls.JFXButton;
import com.jfoenix.controls.JFXComboBox;
import com.jfoenix.controls.JFXDrawer;
import controller.scene_controller.LoadingAnchorPane;
import controller.scene_controller.LoginSceneController;
import controller.stage_controller.StageController;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;
import javafx.beans.value.ObservableValue;
import javafx.event.Event;
import javafx.fxml.FXML;
import javafx.fxml.FXMLLoader;
import javafx.fxml.Initializable;
import javafx.scene.Cursor;
import javafx.scene.control.Label;
import javafx.scene.control.PasswordField;
import javafx.scene.control.TextField;
import javafx.scene.image.Image;
import javafx.scene.image.ImageView;
import javafx.scene.layout.AnchorPane;
import javafx.scene.layout.VBox;
import javafx.scene.paint.Color;
import javafx.stage.Stage;
import model.APIs.ValidateAPIs.ValidateInput;

/**
 * FXML Controller class
 *
 * @author User
 */
public class Sign_up_step4_SceneController implements Initializable {

    @FXML
    private AnchorPane rootPane;
    @FXML
    private AnchorPane background;
    @FXML
    private JFXButton btnMenu;
    @FXML
    private JFXButton btnHome;
    @FXML
    private JFXButton btnAbout;
    @FXML
    private JFXButton btnTicketing;
    @FXML
    private JFXButton btnSearch;
    @FXML
    private JFXButton btnHelp;
    @FXML
    private TextField txtCardHolderName;
    @FXML
    private PasswordField txtSecurityCode;
    @FXML
    private TextField txtCardNumber;
    @FXML
    private JFXComboBox<?> cbMonth;
    @FXML
    private JFXComboBox<?> cbYear;
    @FXML
    private JFXButton btnSubmitSignUpForm;
    @FXML
    private JFXButton btnSubmitSignUpFormWithoutPaymentInfo;
    @FXML
    private JFXButton btnToStep3_Side;
    @FXML
    private Label lblCardHolderNameMessage;
    @FXML
    private Label lblCardNumberMessage;
    @FXML
    private Label lblExpirationDateMessage;
    @FXML
    private Label lblSecurityCodeMessage;
    @FXML
    private ImageView iconWarningCardHolderName;
    @FXML
    private ImageView iconWarningCardNumber;
    @FXML
    private ImageView iconWarningExpirationDate;
    @FXML
    private ImageView iconWarningSecurityCode;
    @FXML
    private JFXDrawer drawer;
    @FXML
    private JFXButton btnToStep3;
    @FXML
    private Label lblCardType;
    @FXML
    private ImageView iconCardType;

    private LoadingAnchorPane loading_anchor_pane;
    private final ValidateInput validation = new ValidateInput();

    /**
     * Initializes the controller class.
     *
     * @param url
     * @param rb
     */
    @Override
    public void initialize(URL url, ResourceBundle rb) {
        // TODO

        //Add loading anchor pane
        loading_anchor_pane = new LoadingAnchorPane();
        try {
            loading_anchor_pane.configure_pane(1200, 800, "#000000", 0.8);
        } catch (FileNotFoundException ex) {
            Logger.getLogger(LoginSceneController.class.getName()).log(Level.SEVERE, null, ex);
        }
        rootPane.getChildren().add(loading_anchor_pane);
        loading_anchor_pane.toBack();
        loading_anchor_pane.setCursor(Cursor.WAIT);

        //Add non-login side menu
        try {
            VBox box = FXMLLoader.load(getClass().getResource("/view/fxml/sidemenu_notlogin.fxml"));
            drawer.setSidePane(box);
        } catch (IOException e) {
        }

        //Set hand cursor when mouse enters button
        set_hand_cursor_for_button(btnMenu);
        set_hand_cursor_for_button(btnHome);
        set_hand_cursor_for_button(btnAbout);
        set_hand_cursor_for_button(btnTicketing);
        set_hand_cursor_for_button(btnSearch);
        set_hand_cursor_for_button(btnHelp);
        set_hand_cursor_for_button(btnToStep3);
        set_hand_cursor_for_button(btnToStep3_Side);
        set_hand_cursor_for_button(btnSubmitSignUpForm);
        set_hand_cursor_for_button(btnSubmitSignUpFormWithoutPaymentInfo);
        cbMonth.setCursor(Cursor.HAND);
        cbYear.setCursor(Cursor.HAND);

        //Add focus listener to CardHolderName Textfield to validate CardHolderName field when its focus property is lost.
        txtCardHolderName.focusedProperty().addListener((ObservableValue<? extends Boolean> observable, Boolean oldValue, Boolean newValue) -> {
            if (!newValue) {
                //TO-DO when CardHolderName field is lost focused
                String card_holder_name = txtCardHolderName.getText().trim();
                validate_card_holder_name_field(card_holder_name);
            } else {
                //TO-DO when CardHolderName field is focused
                iconWarningCardHolderName.setVisible(false);
                lblCardHolderNameMessage.setText("Tên nhập vào phải trùng với tên ghi trên thẻ.");
                lblCardHolderNameMessage.setTextFill(Color.web("#c5bebe"));
            }
        });

        //Add focus listener to CardNumber Textfield to validate CardNumber field when its focus property is lost.
        txtCardNumber.focusedProperty().addListener((ObservableValue<? extends Boolean> observable, Boolean oldValue, Boolean newValue) -> {
            if (!newValue) {
                //TO-DO when CardNumber field is lost focused
                String card_number = txtCardNumber.getText().trim();
                validate_card_number_field(card_number);
            } else {
                //TO-DO when CardNumber field is focused
                iconWarningCardNumber.setVisible(false);
                lblCardNumberMessage.setText("Mã thẻ không được chứa kí tự đặc biệt như - /");
                lblCardNumberMessage.setTextFill(Color.web("#c5bebe"));
            }
        });

        //Add focus listener to SecurityCode Textfield to validate SecurityCode field when its focus property is lost.
        txtSecurityCode.focusedProperty().addListener((ObservableValue<? extends Boolean> observable, Boolean oldValue, Boolean newValue) -> {
            if (!newValue) {
                //TO-DO when SecurityCode field is lost focused
                String security_code = txtSecurityCode.getText().trim();
                validate_security_code_field(security_code);
            } else {
                //TO-DO when SecurityCode field is focused
                iconWarningSecurityCode.setVisible(false);
                lblSecurityCodeMessage.setText("3 chữ số được in ở mặt sau của thẻ.");
                lblSecurityCodeMessage.setTextFill(Color.web("#c5bebe"));
            }
        });

        //Add change listener to CardNumber TextField to auto in search 2 spaces every 4 characters that user inserted
        //Also identify what type of card that user uses.
        txtCardNumber.textProperty().addListener((ObservableValue<? extends String> observable, String oldValue, String newValue) -> {
            String card_number = txtCardNumber.getText();

            if (card_number.length() != 22) {
                //TO-DO when user has not finished card number yet
                switch (card_number.length()) {
                    case 4:
                        txtCardNumber.setText(card_number + "  ");
                        break;
                    case 10:
                        txtCardNumber.setText(card_number + "  ");
                        break;
                    case 16:
                        txtCardNumber.setText(card_number + "  ");
                        break;
                }
            } else {
                //TO-DO when user has finished card number. Identify card type if user entered valid card number
                String[] card_number_parts = card_number.split("  ");
                card_number = card_number_parts[0] + card_number_parts[1] + card_number_parts[2] + card_number_parts[3];
                System.out.println(card_number);
                if (validation.check_visa(card_number)) {
                    lblCardType.setText("Visa Card");
                    try {
                        Image visa_card_image = new Image(new FileInputStream("src/main/resources/view/icon/visa.png"));
                        iconCardType.setImage(visa_card_image);
                        iconCardType.setVisible(true);
                    } catch (FileNotFoundException ex) {
                        Logger.getLogger(Sign_up_step4_SceneController.class.getName()).log(Level.SEVERE, null, ex);
                    }
                } else if (validation.check_mastercard(card_number)) {
                    lblCardType.setText("Master Card");
                    try {
                        Image master_card_image = new Image(new FileInputStream("src/main/resources/view/icon/maestro.png"));
                        iconCardType.setImage(master_card_image);
                        iconCardType.setVisible(true);
                    } catch (FileNotFoundException ex) {
                        Logger.getLogger(Sign_up_step4_SceneController.class.getName()).log(Level.SEVERE, null, ex);
                    }
                }
            }
        });
    }

    
    //Button action for btnSubmitSignUpForm
    @FXML
    
    //Set hand cursor for all button
    private void set_hand_cursor_for_button(JFXButton button) {
        button.setOnMouseEntered((Event event) -> {
            button.setCursor(Cursor.HAND);
        });
    }

    //Button action for btnMenu
    //Toggle side menu when click on btnMenu
    @FXML
    private void toggle_side_menu() {
        drawer.toggle();
    }

    //Button action for btnHome
    //Display home scene when click on btnHome on the horizontal menu bar
    @FXML
    private void home_button_action() {
        try {
            Stage current_stage = (Stage) btnHome.getScene().getWindow();
            StageController home_stage = new StageController();
            home_stage.configure_stage(current_stage, "/view/fxml/home.fxml", "Minh Nhut Corporation", 1200, 800);
            current_stage.show();
        } catch (IOException ex) {
            Logger.getLogger(LoginSceneController.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    private void validate_card_holder_name_field(String card_holder_name) {
        //Check if the input card holder name is valid or not
        if (validation.check_name(card_holder_name)) {
            //TO-DO when card holder name field is valid
            iconWarningCardHolderName.setVisible(false);
            lblCardHolderNameMessage.setText("");
            lblCardHolderNameMessage.setTextFill(Color.web("#c5bebe"));
        } else {
            //TO-DO when card holder name field is invalid
            iconWarningCardHolderName.setVisible(true);
            lblCardHolderNameMessage.setText("Tên chủ thẻ không hợp lệ.");
            lblCardHolderNameMessage.setTextFill(Color.web("#ff0000"));
        }
    }

    private void validate_card_number_field(String card_number) {
        //Check if the input card number field is valid or not
        if (validation.check_visamastercard(card_number)) {
            //TO-DO when card number field is valid
            iconWarningCardNumber.setVisible(false);
            lblCardNumberMessage.setText("");
            lblCardNumberMessage.setTextFill(Color.web("#c5bebe"));
        } else {
            //TO-DO when card number field is valid
            iconWarningCardNumber.setVisible(false);
            lblCardNumberMessage.setText("Mã thẻ không hợp lệ.");
            lblCardNumberMessage.setTextFill(Color.web("#ff0000"));
        }
    }

    private void validate_security_code_field(String security_code) {
        //Check if the security code is valid or not
        if (validation.check_cvv(security_code)) {
            //TO-DO when the security code field is valid
            iconWarningSecurityCode.setVisible(false);
            lblSecurityCodeMessage.setText("");
            lblSecurityCodeMessage.setTextFill(Color.web("#c5bebe"));
        } else {
            //TO-DO when the security code field is invalid
            iconWarningSecurityCode.setVisible(true);
            lblSecurityCodeMessage.setText("Số bảo mật thẻ không hợp lệ.");
            lblSecurityCodeMessage.setTextFill(Color.web("#ff0000"));
        }
    }

}