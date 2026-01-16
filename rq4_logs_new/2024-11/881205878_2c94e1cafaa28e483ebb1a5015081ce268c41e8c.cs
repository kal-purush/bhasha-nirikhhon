

namespace OfficePerformanceReview.Domain.Common.ValueObjects
{
    public class DateRange : ValueObject
    {
        protected DateRange()
        {

        }
        public DateOnly Start { get; private set; }
        public DateOnly End { get; private set; }

        public DateRange(DateOnly start, DateOnly end)

        {
            Guard.Against.Null(start, nameof(start));
            Guard.Against.Null(end, nameof(end));
            Guard.Against.OutOfRange(start, nameof(start), DateOnly.MinValue, end);
            Start = start;
            End = end;
        }
        protected override IEnumerable<object> GetEqualityComponents()
        {
            yield return Start;
            yield return End;
        }
    }
}