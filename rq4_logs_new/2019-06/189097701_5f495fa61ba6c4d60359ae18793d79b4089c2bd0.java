package ru.fmtk.hlystov.hw_examination_app.domain.examination;

import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;
import ru.fmtk.hlystov.hw_examination_app.domain.examination.question.IQuestion;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;

public class Exam implements IExam {
    @NonNull
    private final List<IQuestion> questions;

    public Exam() {
        questions = new ArrayList<>();
    }

    @Override
    public int questionsNumber() {
        return questions.size();
    }

    @Override
    public void clear() {
        questions.clear();
    }

    @Override
    @Nullable
    public IQuestion getQuestion(int index) {
        if (index < questionsNumber()) {
            return questions.get(index);
        }
        return null;
    }

    @Override
    public void addQuestion(@NonNull IQuestion question) {
        questions.add(question);
    }

    @NonNull
    public Iterator<IQuestion> iterator() {
        return questions.iterator();
    }

    @Override
    public void forEach(Consumer<? super IQuestion> action) {
        questions.forEach(action);
    }

    @Override
    public Spliterator<IQuestion> spliterator() {
        return questions.spliterator();
    }
}