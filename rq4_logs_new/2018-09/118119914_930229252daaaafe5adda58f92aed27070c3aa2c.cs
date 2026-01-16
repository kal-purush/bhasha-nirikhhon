using ProtoBuf;

namespace Lykke.Service.PayInvoice.Contract.Commands
{
    [ProtoContract]
    public class UpdateEmployeeCommand
    {
        [ProtoMember(1, IsRequired = true)]
        public string EmployeeId { get; set; }

        [ProtoMember(2, IsRequired = true)]
        public string MerchantId { get; set; }

        [ProtoMember(3, IsRequired = true)]
        public string Email { get; set; }

        [ProtoMember(4, IsRequired = true)]
        public string FirstName { get; set; }

        [ProtoMember(5, IsRequired = true)]
        public string LastName { get; set; }

        [ProtoMember(6, IsRequired = false)]
        public string Password { get; set; }

        [ProtoMember(7, IsRequired = true)]
        public bool IsBlocked { get; set; }

        [ProtoMember(8, IsRequired = true)]
        public bool IsInternalSupervisor { get; set; }
    }
}