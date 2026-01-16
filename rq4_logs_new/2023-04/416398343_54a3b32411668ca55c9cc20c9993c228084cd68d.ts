import { BreedEntity } from '../../../core/entities/breed.entity'
import { createBreedService } from '../../../infrastructure/services/breed.service'

export default (breedPayload: BreedEntity) => {
  return createBreedService(breedPayload)
}