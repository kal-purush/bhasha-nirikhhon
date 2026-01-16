/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package ir.visoft.accounting.ui.controller;

import ir.visoft.accounting.db.DatabaseUtil;
import ir.visoft.accounting.entity.AccountBalance;
import ir.visoft.accounting.entity.Bill;
import ir.visoft.accounting.exception.DatabaseOperationException;
import static ir.visoft.accounting.ui.controller.BaseController.resourceBundle;
import java.net.URL;
import java.util.List;
import java.util.ResourceBundle;
import java.util.logging.Logger;
import javafx.collections.FXCollections;
import javafx.fxml.FXML;
import javafx.fxml.Initializable;
import javafx.scene.control.TableColumn;
import javafx.scene.control.TableView;
import javafx.scene.control.cell.PropertyValueFactory;

/**
 * FXML Controller class
 *
 * @author Lenovo
 */
public class AccBalanceManagementController extends BaseController {

    private static final Logger LOG = Logger.getLogger(BillManagementController.class.getName());

    @FXML
    private TableView<AccountBalance> accBalanceTable;

    private AccountBalance selectedAcc;

    @FXML
    public void initialize() {
        setAccTableData();
    }
    
    
     private void setAccTableData() {
        List<AccountBalance> Acc = null;
        try {
                            
            Acc = DatabaseUtil.getAll(new AccountBalance());
            
            TableColumn userIdColumn = new TableColumn(resourceBundle.getString("userId"));
            userIdColumn.setCellValueFactory(new PropertyValueFactory("userId"));

            TableColumn accIdColumn = new TableColumn(resourceBundle.getString("accId"));
            accIdColumn.setCellValueFactory(new PropertyValueFactory("accId"));

            TableColumn createDateColumn = new TableColumn(resourceBundle.getString("createDate"));
            createDateColumn.setCellValueFactory(new PropertyValueFactory("createDate"));

            TableColumn debitColumn = new TableColumn(resourceBundle.getString("debit"));
            debitColumn.setCellValueFactory(new PropertyValueFactory("debit"));

            TableColumn creditColumn = new TableColumn(resourceBundle.getString("credit"));
            creditColumn.setCellValueFactory(new PropertyValueFactory("credit"));

            TableColumn accountBalanceColumn = new TableColumn(resourceBundle.getString("accountBalance"));
            accountBalanceColumn.setCellValueFactory(new PropertyValueFactory("accountBalance"));
            
            TableColumn descriptionColumn = new TableColumn(resourceBundle.getString("description"));
            descriptionColumn.setCellValueFactory(new PropertyValueFactory("description"));

           
            accBalanceTable.getItems().removeAll();
            accBalanceTable.getColumns().clear();
            accBalanceTable.getColumns().addAll(userIdColumn , accIdColumn, createDateColumn , debitColumn, creditColumn , accountBalanceColumn , descriptionColumn);
            accBalanceTable.setItems(FXCollections.observableList(Acc));
        } catch (DatabaseOperationException e) {
            showOperationError();
        }
    }
}