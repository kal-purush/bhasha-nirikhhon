package cz.codeland.gunlicencetester;

import org.hibernate.Session;
import org.hibernate.SessionFactory;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.hibernate.service.ServiceRegistry;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class HibernateTest
{
  private SessionFactory sessionFactory;

  @Before
  public void setUp() throws Exception
  {
    Configuration configuration = new Configuration().configure();
    ServiceRegistry serviceRegistry = new StandardServiceRegistryBuilder()
      .applySettings(configuration.getProperties())
      .build();

    sessionFactory = configuration.buildSessionFactory(serviceRegistry);
  }

  @Test
  public void createDatabase_CreatesDatabaseByUsingHibernate_CreatedDatabaseWithQuestionsAndAnswers() throws Exception
  {
    // Given
    Session session = sessionFactory.openSession();
    session.beginTransaction();

    List<Question> expectedQuestions = new ArrayList<>(1);
    Question tmpQuestion = new Question("Otazka 1");
    List<Answer> expectedAnswers = new ArrayList<>(3);
    expectedAnswers.add(new Answer("Odpoved 1").setQuestion(tmpQuestion));
    expectedAnswers.add(new Answer("Odpoved 2").setCorrect(true).setQuestion(tmpQuestion));
    expectedAnswers.add(new Answer("Odpoved 3").setQuestion(tmpQuestion));
    tmpQuestion.setAnswers(expectedAnswers);
    expectedQuestions.add(tmpQuestion);

    for (Question expectedQuestion : expectedQuestions) {
      session.save(expectedQuestion);
    }
    session.getTransaction().commit();

    // When
    session.beginTransaction();
    List actualAnswers = session.createQuery("from Answer").list();
    session.getTransaction().commit();

    // Then
    for (int i = 0; i < actualAnswers.size(); i++) {
      Assert.assertEquals(expectedAnswers.get(i).getText(), ((Answer) actualAnswers.get(i)).getText());
      Assert.assertEquals(expectedAnswers.get(i).isCorrect(), ((Answer) actualAnswers.get(i)).isCorrect());
    }

    session.close();
  }
}