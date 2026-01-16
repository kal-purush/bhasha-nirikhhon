package cz.codeland.gunlicencetester;

import java.util.ArrayList;
import java.util.List;

public class DefaultTextParser implements TextParser
{
  @Override
  public List<Question> parseText(String text)
  {
    List<Question> questions = new ArrayList<>();

    String[] questionAndAnswers = text.split("[a-c]\\) ");
    Question question = new Question(clean(questionAndAnswers[0]));
    question.addAnswer(new Answer(clean(questionAndAnswers[1])));
    question.addAnswer(new Answer(clean(questionAndAnswers[2])));
    question.addAnswer(new Answer(clean(questionAndAnswers[3])));
    questions.add(question);

    return questions;
  }

  private String clean(String text)
  {
    return text.replaceAll("\\d+\\. ", "").replaceAll("\\n", "").trim().replaceAll(",$", "").replaceAll("\\.$", "");
  }
}