module com.fenix.ordenararquivos {
    exports com.fenix.ordenararquivos;
    exports com.fenix.ordenararquivos.controller;
    exports com.fenix.ordenararquivos.model;
    exports com.fenix.ordenararquivos.logback;

    requires transitive javafx.controls;
    requires javafx.fxml;
    requires transitive javafx.graphics;
    requires javafx.base;
    requires transitive com.jfoenix;
    requires java.desktop;
    requires org.flywaydb.core;
    requires java.sql;
    requires org.xerial.sqlitejdbc;
    requires java.logging;
    requires org.slf4j;
    requires logback.classic;
    requires logback.core;

    opens com.fenix.ordenararquivos.controller to javafx.fxml, javafx.graphics;
    opens com.fenix.ordenararquivos.model to javafx.base;
}