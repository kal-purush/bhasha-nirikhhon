/*
* To change this license header, choose License Headers in Project Properties.
* To change this template file, choose Tools | Templates
* and open the template in the editor.
*/
package dao;

import entidades.Usuario;
import entidades.Venta;
import java.util.ArrayList;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;

/**
 *
 * @author wasp
 */
public class daoUsuario {
    private static Session session;
    private static SessionFactory sf;
    
    private static void iniciaOperacion()throws Exception {
        if(session==null){
            sf = HibernateUtil.getSessionFactory();
            session = sf.openSession();
        }
    }
    
    public ArrayList<Usuario> getPass(String pass)throws Exception {
        iniciaOperacion();
        Transaction tx = session.beginTransaction();
        Query query= session.createQuery("From Usuario u where u.passUsuario =:pass");
        query.setParameter("pass",pass);
        ArrayList<Usuario> usuario = (ArrayList<Usuario>) query.list();
        
        session.flush();
        tx.commit();
        return usuario;
    }
    public boolean updatePass(Usuario usu)throws Exception{
        iniciaOperacion();
        Transaction tx = session.beginTransaction();
        session.update(usu);
        session.flush();
        tx.commit();
        return true;
    }
    
    
    
}