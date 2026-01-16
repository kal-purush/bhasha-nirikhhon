import { Pet } from './Mascotas'

export class PetManager {
    private pets: Array<Pet> = []

    addPet(Pet:Pet) {
        this.pets.push(Pet)
    }

    getPets(): Array<Pet> {
        return this.pets
    }

    fedPet(): Array<Pet> {
        return this.pets.filter(pets => pets.feedPet())
    }

    petPlay(): Array<Pet> {
        return this.pets.filter(pets => pets.petPlay())
    }

    petCare():Array<Pet>{
        return this.pets.filter(pets => pets.petCare())
    }
}


 const mascota1 = new Pet({
    name:'Marley',
    life:100,
    breed:'Calle con vereda',
    cared:'cuidado',
    happiness:100,
    hunger:100
 })

 const mascotList = new PetManager()
 mascotList.addPet(mascota1)

 console.log(mascotList.getPets())