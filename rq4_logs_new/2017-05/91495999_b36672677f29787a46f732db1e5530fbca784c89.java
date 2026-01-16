package com.gv.app.services.impls;

import com.gv.app.models.Student;
import com.gv.app.services.interfaces.StudentService;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;
import org.hibernate.query.Query;

public class StudentServiceHibernate implements StudentService{

    private SessionFactory factory;

    public StudentServiceHibernate(){
        factory = new Configuration()
                .configure("hibernate/hibernate.cfg.xml")
                .addAnnotatedClass(Student.class)
                .buildSessionFactory();
    }

    public boolean addStudent(Student student) {
        final Session session = factory.openSession();
        boolean isAdded = false;
        session.beginTransaction();
        try {
            session.save(student);
            isAdded = true;
        } finally {
            session.getTransaction().commit();
            session.close();
            return isAdded;
        }
    }

    public Student getStudent(int id) {
        final Session session = factory.openSession();
        Student student = null;
        session.beginTransaction();
        try {
            student = session.get(Student.class, id);
        } finally {
            session.getTransaction().commit();
            session.close();
            return student;
        }
    }

    public int updateStudent(int id, Student student) {
        final Session session = factory.openSession();
        session.beginTransaction();
        int rowsEffected = 0;
        try {
            String hql = "UPDATE Student set first_name = :first_name , last_name = :last_name , email = :email WHERE id = :id";
            Query query = session.createQuery(hql);
            query.setParameter("first_name", student.getFirstName());
            query.setParameter("last_name", student.getLastName());
            query.setParameter("email", student.getEmail());
            query.setParameter("id", id);
            rowsEffected = query.executeUpdate();
        } finally {
            session.getTransaction().commit();
            session.close();
            return rowsEffected;
        }
    }

    public boolean deleteStudent(int id) {
        final Session session = factory.openSession();
        session.beginTransaction();
        boolean isDeleted = false;
        try{
            Query query = session.createQuery("DELETE from Student where id = :id");
            query.setParameter("id", id);
            query.executeUpdate();
            isDeleted = true;
        } finally {
            session.getTransaction().commit();
            session.close();
            return isDeleted;
        }
    }
}