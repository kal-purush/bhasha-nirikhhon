namespace CSL;

using Antlr4.Runtime.Misc;

public class EventVisitor : CSLBaseVisitor<Event>
{
    public override Event VisitSubject(CSLParser.SubjectContext context)
    {
        var subject = new SubjectVisitor().VisitSubject(context);
        
        return new Event(subject: subject);
    }

    public override Event VisitDate(CSLParser.DateContext context)
    {
        var date = new DateVisitor().VisitDate(context);
        
        return new Event(date: date);
    }

    public override Event VisitClock(CSLParser.ClockContext context)
    {
        var clock = new ClockVisitor().VisitClock(context);
        
        return new Event(clock: clock);
    }

    public override Event VisitDuration(CSLParser.DurationContext context)
    {
        var duration = new DurationVisitor().VisitDuration(context);
        
        return new Event(duration: duration);
    }

    public override Event VisitDescription(CSLParser.DescriptionContext context)
    {
        var description = new DescriptionVisitor().VisitDescription(context);

        return new Event(description: description);
    }
}