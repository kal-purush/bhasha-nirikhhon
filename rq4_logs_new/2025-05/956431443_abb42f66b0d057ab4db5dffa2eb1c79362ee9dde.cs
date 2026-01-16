using CSL.Exceptions;

namespace CSL;

[Flags]
public enum EventTypes
{
    Calendar = 0,
    Subject = 0b1,
    Date = 0b10,
    Clock = 0b100,
    Description = 0b1000,
    Duration = 0b10000,
}

public class TypeCheckerVisitor : CSLBaseVisitor<EventTypes>
{
    public override EventTypes VisitClock(CSLParser.ClockContext context)
    {
        return EventTypes.Clock | EventTypes.Duration;
    }

    public override EventTypes VisitAddOp(CSLParser.AddOpContext context)
    {
        var left = Visit(context.expr(0));
        var right = Visit(context.expr(1));

        if (left == EventTypes.Duration && right is EventTypes.Duration)
            return EventTypes.Duration;

        if (left is EventTypes.Duration && right == (EventTypes.Clock | EventTypes.Date))
            return EventTypes.Clock;
        
        if (left is EventTypes.Duration && right is EventTypes.Date)
            return EventTypes.Date;
        
        throw new InvalidTypeCompilerException("");
    }
}