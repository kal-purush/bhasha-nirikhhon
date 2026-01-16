package bank.gui;

import bank.model.BankInterestReply;
import bank.model.BankInterestRequest;
import bank.pattern.LoanBrokerAppGateway;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.ListView;
import javafx.scene.control.TextField;

import java.net.URL;
import java.util.ResourceBundle;

public class BankController implements Initializable {
    private final String BANK_ID = "ING";

    private LoanBrokerAppGateway gateway = new LoanBrokerAppGateway() {
        @Override
        public void onLoanRequestArrived(BankInterestRequest request) {
            lvBankRequestReply.getItems().add(new ListViewLine(request));
        }
    };
    public ListView<ListViewLine> lvBankRequestReply;
    public TextField tfInterest;

    @FXML
    public void btnSendBankInterestReplyClicked() {
        double interest = Double.parseDouble(tfInterest.getText());
        BankInterestReply bankInterestReply = new BankInterestReply(interest, BANK_ID);
        gateway.sendBankInterest(bankInterestReply,lvBankRequestReply.getSelectionModel().getSelectedItem().getBankInterestRequest());
        lvBankRequestReply.getSelectionModel().getSelectedItem().setBankInterestReply(bankInterestReply);
        lvBankRequestReply.refresh();
    }

    @Override
    public void initialize(URL location, ResourceBundle resources) {

    }
}