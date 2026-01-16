/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Attendance_Data_Access;

import Connection.DBConnector;
import java.util.Date;

/**
 *
 * @author Pasindu Tennage
 */
public abstract class Attendace_Data_Access {
    protected DBConnector connector ; 
    public Attendace_Data_Access(DBConnector connector) {
        this.connector = connector ;
    }    
}