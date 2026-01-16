package ru.fmtk.hlystov.examinationapp.services.examinator;

import org.springframework.context.annotation.Profile;
import org.springframework.shell.standard.ShellComponent;
import ru.fmtk.hlystov.examinationapp.domain.statistics.ExamStatistics;
import ru.fmtk.hlystov.examinationapp.services.presenter.Presenter;

@Profile("shell")
@ShellComponent
public class SellExaminator implements Examinator {
    @Override
    public void performExam() {

    }

    @Override
    public ExamStatistics getStatistics() {
        return null;
    }

    @Override
    public Presenter getPresenter() {
        return null;
    }
}