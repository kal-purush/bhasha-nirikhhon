using System;
using System.ComponentModel.DataAnnotations.Schema;

namespace Entities
{
	[Table("InOutHistory")]
	public class InOutHistory : EntityBase
	{
		[ForeignKey("UserInfo")]
		public Guid UserId { get; set; }
		public virtual UserInfo UserInfo { get; set; }

		[ForeignKey("Door")]
		public Guid DoorId { get; set; }
		public virtual Door Door { get; set; }

        [ForeignKey("ActionStatus")]
        public Guid ActionStatusId { get; set; }
		public virtual ActionStatus ActionStatus { get; set; }
	}
}
