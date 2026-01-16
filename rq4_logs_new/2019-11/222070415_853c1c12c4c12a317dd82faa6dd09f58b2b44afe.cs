using System;
using ServiceStack.DataAnnotations;

namespace macdream.api.database
{
	public class VisaMccTbl
	{
		[AutoIncrement]
		[PrimaryKey]
		public long Id { get; set; }

		[Required]
		//[StringLength(4, 4)]
		public VisaMccEnum VisaMcc { get; set; } = VisaMccEnum.Missing;

        public bool isSaving { get; set; } = false;
	}
}