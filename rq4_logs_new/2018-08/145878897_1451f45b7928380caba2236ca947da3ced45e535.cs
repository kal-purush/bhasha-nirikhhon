using AnimalSalvationArmy.DataAccessLayer.DataHelpers;
using AnimalSalvationArmy.DataAccessLayer.Entities;
using AnimalSalvationArmy.Services.Entities;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace AnimalSalvationArmy.DataAccessLayer
{
    public class AnimalShelterApplicationDataStore
    {
        public ICollection<AdoptionContactEntity> AdoptionContact { get; set; }
        public virtual ICollection<AnimalShelterEntity> AnimalShelters { get; set; }
        public ICollection<PetEntity> Pets { get; set; }
        public ICollection<ShelterWorkerEntity> ShelterWorkers { get; set; }
        private IUniqueIdentityGenerator _uniqueIdentityGenerator;

        public AnimalShelterApplicationDataStore(IUniqueIdentityGenerator uniqueIdentityGenerator)
        {
            AdoptionContact = new List<AdoptionContactEntity>();
            AnimalShelters = new List<AnimalShelterEntity>();
            Pets = new List<PetEntity>();
            ShelterWorkers = new List<ShelterWorkerEntity>();
            _uniqueIdentityGenerator = uniqueIdentityGenerator;
        }
        public void AddAnimalShelter(AnimalShelterEntity animalShelterEntity)
        {
            animalShelterEntity.Id = _uniqueIdentityGenerator.GetIdentityForList( AnimalShelters.Select(x=> (BaseEntity)x).ToList());
            AnimalShelters.Add(animalShelterEntity);
        }
        public void AddAdoptionContact(AdoptionContactEntity adoptionContactEntity)
        {
            adoptionContactEntity.Id = _uniqueIdentityGenerator.GetIdentityForList(AdoptionContact.Select(x => (BaseEntity)x).ToList());
            AdoptionContact.Add(adoptionContactEntity);
        }
        public void AddPet(PetEntity petEntity)
        {
            petEntity.Id = _uniqueIdentityGenerator.GetIdentityForList(Pets.Select(x => (BaseEntity)x).ToList());
            Pets.Add(petEntity);
        }
        public void AddShelterWorker(ShelterWorkerEntity shelterWorkerEntity)
        {
            shelterWorkerEntity.Id = _uniqueIdentityGenerator.GetIdentityForList(ShelterWorkers.Select(x => (BaseEntity)x).ToList());
            ShelterWorkers.Add(shelterWorkerEntity);
        }

    }
 
 
}