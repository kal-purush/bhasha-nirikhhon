package com.seleniumexpress.hibernate;

import java.util.List;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.Transaction;
import org.hibernate.query.Query;

import com.seleniumexpress.models.Address;
import com.seleniumexpress.models.Student;
import com.seleniumexpress.utils.HibernateUtils;

/**
 * Hello world!
 *
 */
public class StudentApp {

	private static Session session = null;

	public static void main(String[] args) {

		// 1. create configuration
		// 2. create session factory
		// 3. initialize the session object

		SessionFactory sessionFactory = HibernateUtils.getSessionFactory();
		session = sessionFactory.openSession();
		/*
		 * printNameandAgeAllStudents(); findStudentByAge(44); findStudentByAge(20, 40);
		 * findAll(); getAllStudents(); findAllStudentByAge(44); findAllNative();
		 * findAverageAgeByCountry("INDIA"); updateAllStudentByOne();
		 */

		Transaction transaction = session.getTransaction();
		transaction.begin();
		Student student = new Student();
		student.setAge(28);
		student.setCountry("INDIA");
		student.setMobileNo(9450549618l);
		student.setName("PRASHANT1 KUMAR-1");
		student.addNickName("abc-1");
		student.addNickName("gandu-1");
		student.addNickName("bhosadika-1");

		List<Address> addresses = student.getAddresses();
		Address add1 = new Address();
		add1.setAddressLine1("INDIRA NAGAR");
		add1.setAddressLine2("ASHWIN NAGAR");
		add1.setCity("BGLR");
		add1.setCountry("INDIA");

		addresses.add(add1);

		Address add2 = new Address();
		add2.setAddressLine1("INDIRA NAGAR-2");
		add2.setAddressLine2("ASHWIN NAGAR-2");
		add2.setCity("BGLR-2");
		add2.setCountry("INDIA");

		addresses.add(add2);

		session.save(student);
		transaction.commit();

		session.close();
		sessionFactory.close();

	}

	private static void updateAllStudentByOne() {

		Transaction transaction = session.getTransaction();

		transaction.begin();

		int rowsUpdated = session.createQuery("update Student s set  s.age = s.age+1").executeUpdate();

		transaction.commit();
		System.out.println("Update Count :: " + rowsUpdated);
	}

	private static void findAverageAgeByCountry(String string) {

		List<Object[]> avgByCountry = session
				.createQuery("select s.country , avg(s.age) from Student s group by s.country", Object[].class).list();

		for (Object[] obj : avgByCountry) {

			System.out.println("Country :::" + obj[0] + " Average age :::" + obj[1]);
		}

	}

	private static void findAllNative() {
		List<Student> students = session
				.createNativeQuery("select * from students where name = 'Prashant'", Student.class).list();

		students.forEach(System.out::println);

	}

	private static void findAllStudentByAge(int age) {
		List<Student> students = session.createNamedQuery("allstudents.age", Student.class).setParameter("AGE", age)
				.list();

		students.forEach(System.out::println);

	}

	private static void findAll() {
		List<Student> studentList = session.createNamedQuery("allstudents", Student.class).list();
		studentList.forEach(System.out::println);
	}

	private static void printNameandAgeAllStudents() {

		String hql = " SELECT s.name , s.age from  Student s";

		Query<Object[]> query = session.createQuery(hql, Object[].class);

		List<Object[]> list = query.list();

		if (list.isEmpty()) {
			System.out.println("Found no such records ...............");
		}

		for (Object[] obj : list) {
			System.out.println("FirstName ::: " + obj[0] + " Age ::: " + obj[1]);
		}

	}

	private static void findStudentByAge(int minAge, int maxAge) {
		String hql = "from Student s where s.age between :MIN_AGE and :MAX_AGE";

		Query<Student> query = session.createQuery(hql, Student.class);
		query.setParameter("MIN_AGE", minAge);
		query.setParameter("MAX_AGE", maxAge);

		List<Student> list = query.list();

		if (list.isEmpty()) {
			System.out.println("Found no such records ...............");
		}

		for (Student s : list) {
			System.out.println(s);
		}

	}

	private static void findStudentByAge(int age) {
		String hql = "from Student s where s.age >= ?1";

		Query<Student> query = session.createQuery(hql, Student.class);
		query.setParameter(1, age);

		List<Student> list = query.list();

		if (list.isEmpty()) {
			System.out.println("Found no such records ...............");
		}

		for (Student s : list) {
			System.out.println(s);
		}
	}

	private static void getAllStudents() {
		Transaction transaction = session.getTransaction();

		transaction.begin();

		Query<Student> query = session.createQuery("select s from Student s", Student.class);

		List<Student> list = query.list();

		for (Student s : list) {
			System.out.println(s);
		}

		transaction.commit();
	}

}