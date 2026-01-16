using PetOwner.Services.Interfaces;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace PetOwner.Services.Implementations
{
	public class GroupService : IGroupService
	{
		public string GenerateInviteCode(int size)
		{
			const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

			var invitecode = new char[size];

			var random = new Random();

			for(int i = 0; i < size; i++)
			{
				invitecode[i] = chars[random.Next(chars.Length)];
			}

			return new string(invitecode);
		}
	}
}