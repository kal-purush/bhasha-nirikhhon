/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package controller;

import java.util.List;
import model.AreaModel;
//import src.com.syntel.Models.AreaModel;

/**
 *
 * @author syntel
 */
public class AreaManagementController {
    AreaModel am = new AreaModel();

    public boolean addArea(int zip){
        return am.addArea(zip);
    }
    public boolean removeArea(int zip){
        return am.removeArea(zip);
    }
    public List<String> getAreas(){
        return am.getAreas();
    }
    public List<String> getFoodInAreas(int zip){
        return am.getFoodInAreas(zip);
    }
    public boolean addPackagetoArea(int zip,String packageNo){
        return am.addPackagetoArea(zip, packageNo);
    }
    public boolean removePackageFromArea(int zip,String packageNo){
        return am.removePackageFromArea(zip,packageNo);
    }
}