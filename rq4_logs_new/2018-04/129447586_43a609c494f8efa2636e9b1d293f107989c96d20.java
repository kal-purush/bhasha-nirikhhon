package sample;

public class Execution {
    boolean recordProgress(Assignment assg, Double progress)
    {
        assg.setProgress(progress);
        //TODO record the progress in the DB, if error, return false
        return true;
    }
    double viewProgress(Assignment assg)
    {
        return assg.getProgress();
    }
    Feedback recordFeedback(Assignment assg, Resource resource, String remark)
    {
        Feedback fb =   new Feedback(assg, resource, remark);
        //TODO Store the feedback in the DB
        return fb;
    }
    Feedback viewFeedback(Assignment assg)
    {
        //TODO fetch list of feedbacks, select whatevery feedbacks are there for given assignment and return
        return null;
    }
}