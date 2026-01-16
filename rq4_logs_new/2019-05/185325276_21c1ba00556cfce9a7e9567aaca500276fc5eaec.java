/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package DAO;

import Model.*;
import Repository.StaffRepository;
import java.util.ArrayList;
import javax.enterprise.inject.Default;
import javax.inject.Inject;

/**
 *
 * @author clint
 */
@Default
public class OmoaDaoImpl implements OmoaDao {
    
    private StaffRepository staffRepository;

    public OmoaDaoImpl() {
        staffRepository = new StaffRepository();
    }

    @Override
    public ArrayList<Staff> getAllStaff() {
        System.out.println("Inside getAllStaff()");
        return staffRepository.getAllStaff();
    }
    
}