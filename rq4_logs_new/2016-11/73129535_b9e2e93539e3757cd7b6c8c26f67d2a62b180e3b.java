/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package GUI;
import java.io.File;
/**
 *
 * @author nichambers
 */
public class Appointment {
    
    String _name;
    String _date;
    String _time;
    String _length;
    String _fellow;
    File _draft;
    
    public Appointment(String name, String date, String time, String length, String fellow, File draft){
        _name = name;
        _date = date;
        _time = time;
        _length = length;
        _fellow = fellow;
        _draft = draft;
    }
    
    public String toString(){
        return  "<html>" + _name + "-       -" + _date + "<br>" + _time + "-       -" 
                + _length + "<br>" + _fellow + "-       -" + _draft.getName() + "</html>";
    }
    
}