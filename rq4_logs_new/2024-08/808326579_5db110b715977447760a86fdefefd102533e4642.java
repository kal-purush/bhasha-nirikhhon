package App;


import model.Item;
import model.Person;
import org.hibernate.Hibernate;
import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.cfg.Configuration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class App {
    public static void main(String[] args) {
        Configuration configuration = new Configuration().addAnnotatedClass(Person.class).addAnnotatedClass(Item.class); // Добавление 2х сущностей


        SessionFactory sessionFactory = configuration.buildSessionFactory();


        try {
            Session session = sessionFactory.getCurrentSession(); // Получили сессию для работы с Hibernate
            session.beginTransaction(); //Начинаем транзакцию

//            Person person = session.get(Person.class,1);
//            System.out.println("Получили человека");
//
//            //Получим связанные сущности                       <--- Ленивая загрузка, получаем (1)
//            System.out.println(person.getItems());                  товары только после запроса

//            Item item = session.get(Item.class,1);
//            System.out.println("Получили товар");     <-- не ленивая загрузка. Получили всю (2)
//                                                         о человека запросив информацию о товаре
//            System.out.println(item.getOwner());

//          Добавили в аргумент @OneToMany(mappedBy = "owner", fetch = FetchType.EAGER)
            Person person = session.get(Person.class, 1);
            System.out.println("Получили человека из таблицы");
//            System.out.println(person);          //  <--- Теперь загрузка стала не ленивой (3)
//
// Запрос товаров при ленивой загрузке, чтобы можно было запрашивать товары вне сессии без ошибок
//            System.out.println(person.getItems());  // не будет работать без sout
//
//            Hibernate.initialize(person.getItems()); //Спец метод чтобы не использовать sout (4)


            session.getTransaction().commit();
//         session.close() происходит автоматически после commit()

//            // Вне сессии товары можно получать, так как они уже были подгружены (3)
//            System.out.println(person.getItems()); // Возможно то при НЕ ленивой загрузке

            System.out.println("Сессия закончилась");

            //Открываем сессию и транзакцию еще раз (Возможно сделать в любом месте в коде)
            session = sessionFactory.getCurrentSession();
            session.beginTransaction();

            System.out.println("Внутри второй транзакции");

            person = (Person) session.merge(person); //Метод merge "пристегивает объект к новой сессии,
            // так как сессии между собой не связаны и новая сессия не знает о существовании
            // объекта из первой сессии. А поскольку метод возвращает объект класса Object его
            // нужно задаункастить до Person
            Hibernate.initialize(person.getItems()); // Получение товаров через Hibernate (4)

            // 2 способ получить товары после завершения сессии выполнить HQL запрос вручную
            // Пристегивать объект к новой сессии в этом случае не нужно. Используется редко(5)
            List<Item> items = session.createQuery("select i from Item i where i.owner.id=:personId", Item.class).
                    setParameter("personId",person.getId()).getResultList();
            System.out.println(items);


            session.getTransaction().commit();

            System.out.println("Все второй сессии");

            //Это работает, так как связанные товары были загружены (4)
            System.out.println(person.getItems());
        } finally {
            sessionFactory.close();
        }
    }
}