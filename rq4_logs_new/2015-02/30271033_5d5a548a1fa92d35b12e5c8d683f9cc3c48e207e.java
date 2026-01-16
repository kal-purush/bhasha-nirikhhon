package cz.codeland.gunlicencetester;

import java.util.ArrayList;
import java.util.List;

public class Exam
{
  private List<Question> questions = new ArrayList<>();

  public static String printQuestion(Exam exam, int questionIndex)
  {
    StringBuilder result = new StringBuilder();

    final String EOL = System.getProperty("line.separator");
    Question question = exam.getQuestions().get(questionIndex - 1);
    result.append(questionIndex);
    result.append(". ");
    result.append(question.getText());
    result.append(EOL);
    char orderedIndex = 'a';
    for (Answer answer : question.getAnswers()) {
      result.append(" ");
      result.append(orderedIndex);
      result.append(") ");
      result.append(answer.getText());
      result.append(EOL);

      orderedIndex++;
    }

    return result.toString();
  }

  public Exam addQuestion(Question question)
  {
    questions.add(question);
    return this;
  }

  public List<Question> getQuestions()
  {
    return questions;
  }
}