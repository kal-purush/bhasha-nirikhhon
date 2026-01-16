package controllers;

import helpers.Consts;
import helpers.ViewHelper;
import javafx.application.Platform;
import javafx.collections.FXCollections;
import javafx.collections.ObservableList;
import javafx.fxml.FXML;
import javafx.scene.control.*;
import javafx.scene.control.cell.PropertyValueFactory;
import javafx.scene.control.cell.TextFieldTableCell;
import javafx.util.converter.DoubleStringConverter;
import models.Employee;
import models.Tuple;
import services.EmployeeService;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MainController {

    public TableView<Employee> employeers_table;

    public TableColumn<Employee, String> name;
    public TableColumn<Employee, String> email;
    public TableColumn<Employee, Double> salary;

    public Label name_validation;
    public Label email_validation;
    public Label salary_validation;
    public Label danger_label;

    public TextField email_input;
    public TextField salary_input;
    public TextField name_input;

    private ObservableList<Employee> employees;

    private EmployeeService employeeService;
    private ViewHelper viewHelperInstance;

    @FXML
    public void initialize() {
        try {
            employeeService = new EmployeeService();
            viewHelperInstance = new ViewHelper();
            ExecutorService threadWorker = Executors.newFixedThreadPool(1);
            employees = FXCollections.observableArrayList();
            employeers_table.getSelectionModel().setSelectionMode(SelectionMode.MULTIPLE);
            InitializeEmployeesTable();
            threadWorker.submit(this::FillEmployeesList);
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }

    @FXML
    public void addEmployee() {
        try {
            viewHelperInstance.ResetErrors(this);

            String name = name_input.getText();
            String email = email_input.getText();

            if (!viewHelperInstance.IsNameValid(name)) {
                name_validation.setVisible(true);
                return;
            }

            if (!viewHelperInstance.IsEmailValid(email)) {
                email_validation.setVisible(true);
                return;
            }

            Tuple<Boolean, Double> salary = viewHelperInstance.TryGetSalary(salary_input.getText());

            if (!salary.Item1) {
                salary_validation.setText("Pensja została wpisana w złym formacie");
                salary_validation.setVisible(true);
                return;
            }

            if (!employeeService.Add(new Employee(name, email, salary.Item2))) {
                danger_label.setVisible(true);
            }
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
            danger_label.setVisible(true);
        }
    }

    @FXML
    public void deleteEmployees(){
        //TODO Dokończyć funkcjonalność
        ObservableList<Employee> selectedRows = employeers_table.getSelectionModel().getSelectedItems();
        employees.removeAll(selectedRows);
    }

    @FXML
    public void onTextColumnValueChanged(TableColumn.CellEditEvent<Employee, String> editEvent) {
        try {
            viewHelperInstance.ResetErrors(this);
            Employee editedEmp = employeers_table.getSelectionModel().getSelectedItem();

            if (editEvent.getTableColumn().getId().equals(Consts.employeeNameColumnName)) {
                UpdateName(editedEmp, editEvent.getOldValue(), editEvent.getNewValue());
                return;
            }

            UpdateEmail(editedEmp, editEvent.getOldValue(), editEvent.getNewValue());
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }

    @FXML
    public void onSalaryColumnValueChanged(TableColumn.CellEditEvent<Employee, Double> editEvent) {
        try {
            viewHelperInstance.ResetErrors(this);
            Employee editedEmp = employeers_table.getSelectionModel().getSelectedItem();
            UpdateSalary(editedEmp, editEvent.getNewValue());
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        }
    }

    private void UpdateSalary(Employee employee, Double newValue) {

        if (newValue == null) {
            salary_validation.setText("Pensja nie może być pusta");
            salary_validation.setVisible(true);
            employeers_table.refresh();
            return;
        }

        employee.setSalary(newValue);
        employeeService.Update(employee);
    }

    private void UpdateName(Employee employee, String oldValue, String newValue) {
        if (!viewHelperInstance.IsNameValid(newValue) || oldValue.equals(newValue)) {
            name_validation.setVisible(true);
            employeers_table.refresh();
            return;
        }

        employee.setName(newValue);
        employeeService.Update(employee);
    }

    private void UpdateEmail(Employee employee, String oldValue, String newValue) {
        if (!viewHelperInstance.IsEmailValid(newValue) || oldValue.equals(newValue)) {
            email_validation.setVisible(true);
            employeers_table.refresh();
            return;
        }

        employee.setEmail(newValue);
        employeeService.Update(employee);
    }

    private void InitializeEmployeesTable() {
        name.setCellValueFactory(new PropertyValueFactory<>(Consts.employeeNameColumnName));
        email.setCellValueFactory(new PropertyValueFactory<>(Consts.employeeEmailColumnName));
        salary.setCellValueFactory(new PropertyValueFactory<>(Consts.employeeSalaryColumnName));

        employeers_table.setItems(employees);
        employeers_table.setEditable(true);

        name.setCellFactory(TextFieldTableCell.forTableColumn());
        email.setCellFactory(TextFieldTableCell.forTableColumn());
        salary.setCellFactory(TextFieldTableCell.forTableColumn(new DoubleStringConverter()));
    }

    private void FillEmployeesList() {
        List<Employee> rows = employeeService.findAll();

        if (rows != null && rows.size() != 0) {
            Platform.runLater(() -> employees.addAll(rows));
        }
    }
}