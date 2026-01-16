using AnimalSalvationArmy.Services.DataTransferObjects;
using AnimalSalvationArmy.Services.PetService;
using System;
using System.Collections.Generic;
using System.Text;

namespace AnimalSalvationArmy.Services.PetService
{
    public class PetService : IPetService
    {
        public int AddPetToShelter(PetDto newPet)
        {
            throw new NotImplementedException();
        }

        public ICollection<PetDto> GetListOfPets(int? shelterId, bool? petAlreadyPendingAdoption)
        {
            throw new NotImplementedException();
        }

        public PetDto GetPetById(int id)
        {
            throw new NotImplementedException();
        }

        public void RemovePetFromShelter(int petId)
        {
            throw new NotImplementedException();
        }
    }
}