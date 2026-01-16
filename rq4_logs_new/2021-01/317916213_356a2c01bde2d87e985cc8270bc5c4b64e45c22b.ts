import { Attributes } from "../Models/PlayerCharacter"

export const mockAttributes = (): Attributes => ({
  Agility    : { value: 0, bonus: 0 }, Fortitude  : { value: 0, bonus: 0 },
  Might      : { value: 0, bonus: 0 }, Learning   : { value: 0, bonus: 0 },
  Logic      : { value: 0, bonus: 0 }, Perception : { value: 0, bonus: 0 },
  Will       : { value: 0, bonus: 0 }, Deception  : { value: 0, bonus: 0 },
  Persuasion : { value: 0, bonus: 0 }, Presence   : { value: 0, bonus: 0 },
  Alteration : { value: 0, bonus: 0 }, Creation   : { value: 0, bonus: 0 },
  Energy     : { value: 0, bonus: 0 }, Entropy    : { value: 0, bonus: 0 },
  Influence  : { value: 0, bonus: 0 }, Movement   : { value: 0, bonus: 0 },
  Prescience : { value: 0, bonus: 0 }, Protection : { value: 0, bonus: 0 },
})

export default mockAttributes