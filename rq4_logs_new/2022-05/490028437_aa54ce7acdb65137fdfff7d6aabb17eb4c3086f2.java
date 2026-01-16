module opt3 {
    requires javafx.controls;
    requires javafx.fxml;

    exports opt3;
    exports opt3.Controller;
    exports opt3.Model;

    opens opt3 to javafx.fxml;
    opens opt3.Model to javafx.fxml;
    opens opt3.Controller to javafx.fxml;
    //opens opt3.Model to javafx.base;
}