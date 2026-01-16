using _FinalProject.Model.Models;
using Data.Interfaces;
using System;
using System.Collections.Generic;
using System.Text;

namespace Data.Implementations.EFCoreRepositories
{
    public class EFCoreLetterRepository : ILetterRepository
    {
        public Letter Create(Letter newLetter)
        {
            throw new NotImplementedException();
        }

        public bool DeleteById(int letterId)
        {
            throw new NotImplementedException();
        }

        public Letter GetById(int letterId)
        {
            throw new NotImplementedException();
        }

        public ICollection<Letter> GetRobinById(int robinId)
        {
            throw new NotImplementedException();
        }

        public ICollection<Letter> GetUserById(string userId)
        {
            throw new NotImplementedException();
        }

        public Letter Update(Letter updatedLetter)
        {
            throw new NotImplementedException();
        }
    }
}