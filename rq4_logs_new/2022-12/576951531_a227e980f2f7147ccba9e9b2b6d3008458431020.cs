using System.ComponentModel.DataAnnotations.Schema;

namespace EasyAgenda.Model.DTO
{
    [Table("SCHEDULESRESERVED")]
    public class ScheduleReservedDTO
    {
        public int CustomerId { get; set; }
        public int ProfessionalId { get; set; }
        public int AgendaId { get; set; }
        public DateTime Registered { get; set; }

        public ScheduleReservedDTO(int customerId, int professionalId, int agendaId)
        {
            CustomerId = customerId;
            ProfessionalId = professionalId;
            AgendaId = agendaId;
            Registered = DateTime.Now;
        }

    }
}