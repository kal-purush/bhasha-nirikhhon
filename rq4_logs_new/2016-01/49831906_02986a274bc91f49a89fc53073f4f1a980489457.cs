
namespace BerlinClock
{
    public class Clock
    {
        #region fields
        const string firstRow_seconds_const = "O";
        const string secondRow_hours_const = "OOOO";
        const string thirdRow_hours_const = "OOOO";
        const string forthRow_minutes_const = "OOOOOOOOOOO";
        const string fifthRow_minutes_const = "OOOO";

        string firstRow_seconds;
        string secondRow_hours;
        string thirdRow_hours;
        string forthRow_minutes;
        string fifthRow_minutes;
        #endregion

        #region constructors
        public Clock()
        {
            this.FirstRow_seconds = firstRow_seconds_const;
            this.SecondRow_hours = secondRow_hours_const;
            this.ThirdRow_hours = thirdRow_hours_const;
            this.ForthRow_minutes = forthRow_minutes_const;
            this.FifthRow_minutes = fifthRow_minutes_const;
        }
        #endregion

        #region properties
        public string FirstRow_seconds
        {
            get { return firstRow_seconds; }
            set { firstRow_seconds = value; }
        }
        public string SecondRow_hours
        {
            get { return secondRow_hours; }
            set { secondRow_hours = value; }
        }
        public string ThirdRow_hours
        {
            get { return thirdRow_hours; }
            set { thirdRow_hours = value; }
        }
        public string ForthRow_minutes
        {
            get { return forthRow_minutes; }
            set { forthRow_minutes = value; }
        }
        public string FifthRow_minutes
        {
            get { return fifthRow_minutes; }
            set { fifthRow_minutes = value; }
        }
        #endregion

        #region methods
        #endregion
    }
}