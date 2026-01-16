/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package DAO;

import POJO.Usuario;
import UTIL.HibernateUtil;
import java.util.List;
import org.hibernate.Query;
import org.hibernate.Session;
import org.hibernate.Transaction;

/**
 *
 * @author rafael
 */
public class UsuarioDAO {
    
    public boolean cadastrarUsuario(Usuario usuario)
    {
        Transaction trns = null;
        Session session = HibernateUtil.getSessionFactory().openSession();
        
        try
        {
            trns = session.beginTransaction();
            session.save(usuario);
            session.getTransaction().commit();
            return true;
        }
        catch(Exception ex)
        {
            ex.printStackTrace();
            return false;
        }
    }
    
    public boolean realizarLogin (Usuario usuario)
    {
        Transaction trns = null;
        Session session = HibernateUtil.getSessionFactory().openSession();
        
        try
        {
            trns = session.beginTransaction();
            Query query = session.createQuery("From Usuario Where usuarioLogin = :login and usuarioSenha = :senha");
            query.setParameter("login", usuario.getUsuarioLogin());
            query.setParameter("senha", usuario.getUsuarioSenha());
            
            List<Usuario> usu = query.list();
            
            if(!usu.isEmpty())
            {
                return true;
            }
            else
            {
                return false;
            }
        }
        catch(Exception ex)
        {
            ex.printStackTrace();
            return false;
        }
    }
    
    public boolean verificarUsername(Usuario usuario)
    {
        Transaction trns = null;
        Session session = HibernateUtil.getSessionFactory().openSession();
        
        try
        {
            trns = session.beginTransaction();
            Query query = session.createQuery("From Usuario Where usuarioLogin = :login");
            query.setParameter("login", usuario.getUsuarioLogin());
            
            List<Usuario> usu = query.list();
            
            if(usu.isEmpty())
            {
                return true;
            }
            else
            {
                return false;
            }
        }
        catch(Exception ex)
        {
            ex.printStackTrace();
            return false;
        }
    }
    
    public boolean alterarSenha(Usuario usuarioAntigo, Usuario usuarioNovo)
    {
        Transaction trns = null;
        Session sessionSearch = HibernateUtil.getSessionFactory().openSession();
        Session sessionUpdate = HibernateUtil.getSessionFactory().openSession();
        
        try
        {
            trns = sessionSearch.beginTransaction();
            Query query = sessionSearch.createQuery("From Usuario Where usuarioLogin = :login and usuarioSenha = :senha");
            query.setParameter("login", usuarioAntigo.getUsuarioLogin());
            query.setParameter("senha", usuarioAntigo.getUsuarioSenha());
            
            List<Usuario> usu = query.list();
            
            if(!usu.isEmpty())
            {
                trns = sessionUpdate.beginTransaction();
                int codigo = usu.get(0).getUsuarioCodigo();
                usuarioNovo.setUsuarioCodigo(codigo);
                sessionUpdate.update(usuarioNovo);
                sessionUpdate.getTransaction().commit();
                return true;
            }
            else
            {
                return false;
            }
        }
        catch(Exception ex)
        {
            ex.printStackTrace();
            return false;
        }
    }
    
}