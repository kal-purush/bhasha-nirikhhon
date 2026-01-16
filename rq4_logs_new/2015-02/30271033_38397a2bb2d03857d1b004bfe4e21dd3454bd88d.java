package cz.codeland.gunlicencetester;

import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Database
{
  private static final Logger LOGGER = Logger.getLogger(DefaultPDFTextExtractor.class.getName());
  private Connection connection;

  public Database()
  {
    String jdbcDriver = "org.hsqldb.jdbc.JDBCDriver";
    String connectionString = "jdbc:hsqldb:file:ExamQuestions/database";

    try {
      Class.forName(jdbcDriver);
      connection = DriverManager.getConnection(connectionString);
    } catch (Exception e) {
      LOGGER.log(Level.SEVERE, "Failed to load JDBC driver.", e);
    }
  }

  public Connection getConnection()
  {
    return connection;
  }

  public void close() throws SQLException
  {
    connection.close();
  }

  public void generateDatabase() throws IOException, SQLException
  {
    final String absResourcePath = "/questions.pdf";
    DefaultPDFTextExtractor extractor = new DefaultPDFTextExtractor();
    InputStream resourcePdf = extractor.readResourceFile(absResourcePath);
    String extractedText = extractor.extract(resourcePdf, 2);
    resourcePdf.close();

    DefaultTextParser parser = new DefaultTextParser();
    final List<Question> questions = parser.parseText(extractedText);
    createTables();
    persistQuestionsWithAnswers(questions);
  }

  private void createTables() throws SQLException
  {
    Statement statement = connection.createStatement();
    statement.executeUpdate("DROP TABLE IF EXISTS answer");
    statement.executeUpdate("DROP TABLE IF EXISTS question");
    statement.executeUpdate("CREATE TABLE question (id IDENTITY, question VARCHAR(1024) )");
    statement.executeUpdate("CREATE TABLE answer (id IDENTITY, answer VARCHAR(1024) , correct BOOLEAN, question_id INTEGER, FOREIGN KEY (question_id) REFERENCES question(id))");
  }

  private void persistQuestionsWithAnswers(List<Question> questions) throws SQLException
  {
    for (Question question : questions) {
      long lastId = persistQuestion(question);
      persistAnswers(question, lastId);
    }
  }

  private void persistAnswers(Question question, long lastId) throws SQLException
  {
    String SQL_INSERT = "INSERT INTO answer (answer,correct,question_id) VALUES (?,?,?)";
    final PreparedStatement statementA = connection.prepareStatement(SQL_INSERT);
    for (Answer answer : question.getAnswers()) {
      statementA.setString(1, answer.getText());
      statementA.setBoolean(2, answer.isCorrect());
      statementA.setLong(3, lastId);
      statementA.executeUpdate();
    }

    statementA.close();
  }

  private long persistQuestion(Question question) throws SQLException
  {
    String SQL_INSERT = "INSERT INTO question (question) VALUES (?)";
    PreparedStatement statementQ = connection.prepareStatement(SQL_INSERT, Statement.RETURN_GENERATED_KEYS);
    statementQ.setString(1, question.getText());
    statementQ.executeUpdate();
    final ResultSet generatedKeys = statementQ.getGeneratedKeys();
    long lastId = Long.MAX_VALUE;

    if (generatedKeys.next()) {
      lastId = generatedKeys.getLong(1);
    }
    statementQ.close();

    return lastId;
  }
}