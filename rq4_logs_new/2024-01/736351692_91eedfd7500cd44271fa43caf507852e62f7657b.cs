using System;
namespace home_swap_api.Models
{
	public class RefreshToken
	{
		public required string Token { get; set; }
		public DateTime Created { get; set; } = DateTime.Now;
		public DateTime Expires { get; set; }

        public RefreshToken()
		{
			
		}
	}
}
