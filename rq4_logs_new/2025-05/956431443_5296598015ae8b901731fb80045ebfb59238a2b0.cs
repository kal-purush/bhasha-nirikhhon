namespace CSL;

public class CalendarVisitor : CSLBaseVisitor<Calendar>
{
    public override Calendar VisitSubject(CSLParser.SubjectContext context) =>
        new Event(Subject: new SubjectVisitor().VisitSubject(context));

    public override Calendar VisitDate(CSLParser.DateContext context) =>
        new Event(Date: new DateVisitor().VisitDate(context));

    public override Calendar VisitClock(CSLParser.ClockContext context) =>
        new Event(Clock: new ClockVisitor().VisitClock(context));

    public override Calendar VisitDuration(CSLParser.DurationContext context) =>
        new Event(Duration: new DurationVisitor().VisitDuration(context));

    public override Calendar VisitDescription(CSLParser.DescriptionContext context) =>
        new Event(Description: new DescriptionVisitor().VisitDescription(context));
}