/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package eu.atmosphere.tma.admin.controller;

import eu.atmosphere.tma.admin.dto.ActionDashboard;
import eu.atmosphere.tma.admin.util.Constants;
import eu.atmosphere.tma.admin.util.DatabaseManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * This class is a Rest Controller. It handles every request made from the TMA dashboard to the
 * server related with information of Actions and their configurations.
 *
 * @author João Ribeiro  <jdribeiro@student.dei.uc.pt>
 */
@CrossOrigin
@RestController
public class ActionDashboardController {
    private static final Logger LOGGER = LoggerFactory.getLogger(ActionDashboardController.class);
    
    @GetMapping("/getActions")
    public ResponseEntity<Map> getActions(
            @RequestParam(required = false, defaultValue = "0") int resourceId,
            HttpServletResponse response) {

        DatabaseManager database = new DatabaseManager();
        ArrayList<ActionDashboard> actions;
        try {
            actions = database.getActionsDashboard(resourceId);
        } catch (SQLException ex) {
            LOGGER.error("[ATMOSPHERE] Unable to connect to the database", ex);
            return AdminController.genericResponseEntity(Constants.HTTPSERVERERROR, Constants.ERROR, "There was a problem with the connection to the database");
        }

        HashMap<String, ArrayList<ActionDashboard>> actionsJson = new HashMap<>();
        actionsJson.put("actions", actions);

        return new ResponseEntity<>(
                actionsJson,
                HttpStatus.valueOf(response.getStatus())
        );
    }
}