using WolfDen.Domain.Enum;

namespace WolfDen.Domain.Entity
{
    public class LeaveRequest
    {
        public int Id {  get; private set; }
        public int EmployeeId { get;private set; }
        public virtual Employee employee { get; private set; }

        public int TypeId { get;private set; }
        public virtual LeaveType LeaveType { get;private  set; }

        public bool HalfDay { get;private  set; }
        public DateOnly FromDate { get; private set; }
        public DateOnly ToDate { get; private set; }
        public DateOnly ApplyDate { get; private set; }
        public int LeaveRequestStatusId { get; private set; }
        public virtual LeaveRequestStatus LeaveRequestStatus { get; private set; }
        public string Description { get; private set; }
        public int ProcessedBy { get; private set; }
        public virtual Employee Employee { get; private set; }  //managers who can process the LR
        private LeaveRequest() {  }
    }
}