/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package myentity;

import javax.persistence.EntityManager;
import javax.persistence.EntityTransaction;

/**
 *
 * @author midou
 */
public class MyCRUD {
    
    public boolean deleteStudent(int studentId, EntityManager em, EntityTransaction tx)
    {
        boolean b = false;
        Student student = new Student();
        student.setId(studentId);
        //System.out.println(em.contains(student));
        if(em.contains(student) == true)
        {
            tx.begin();
            em.remove(student);
            tx.commit();
            b = true;
        }
        
        //em.close();
        
        //b = em.contains(studentId);
        return b;
    }
}