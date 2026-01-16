import { PcDocument } from "../PlayerCharacter/types"
import { Defense } from "./types"

export const hpRegex = /hp|pv|vida|saude|life|health|healthpoints/i

export const mpRegex = /mp|pm|mana|energia|ki|reyatsu|chackra|chakra/i

export const mockDefense = (): Defense => ({
  value: 0,
  bonus: 0,
})

// defaults set in Models/PlayerCharacter/Schema, as the default values.
// for guard:
//   - base: 0
//   - Might: 1
//   - Fortitude: 0.75
//   - Protection 0.5
// for dodge:
//   - base: 0
//   - Deception: 1
//   - Agility: 0.75
//   - Perception: 0.5
export const getGuard = ({
  guardScaling,
  level,
  attributes,
  highestPhysical,
  highestMental,
  highestSocial,
  highestSpecial,
}: PcDocument): number => {
  const base =
    + guardScaling.base
    + guardScaling.bonus
    + guardScaling.level * level
    
  const physical =
    + guardScaling.Agility    * attributes.Agility   .value
    + guardScaling.Fortitude  * attributes.Fortitude .value
    + guardScaling.Might      * attributes.Might     .value

  const mental =
    + guardScaling.Learning   * attributes.Learning  .value
    + guardScaling.Logic      * attributes.Logic     .value
    + guardScaling.Perception * attributes.Perception.value
    + guardScaling.Will       * attributes.Will      .value
    
  const social =
    + guardScaling.Deception  * attributes.Deception .value
    + guardScaling.Persuasion * attributes.Persuasion.value
    + guardScaling.Presence   * attributes.Presence  .value
    
  const special =
    + guardScaling.Alteration * attributes.Alteration.value
    + guardScaling.Creation   * attributes.Creation  .value
    + guardScaling.Energy     * attributes.Energy    .value
    + guardScaling.Entropy    * attributes.Entropy   .value
    + guardScaling.Influence  * attributes.Influence .value
    + guardScaling.Movement   * attributes.Movement  .value
    + guardScaling.Prescience * attributes.Prescience.value
    + guardScaling.Protection * attributes.Protection.value

  const highest =
    + guardScaling.highestPhysical * highestPhysical .value
    + guardScaling.highestMental   * highestMental   .value
    + guardScaling.highestSocial   * highestSocial   .value
    + guardScaling.highestSpecial  * highestSpecial  .value

  const maxGuard = base + physical + mental + social + special + highest
  return Math.floor(maxGuard)
}

export const getDodge = ({
  dodgeScaling,
  level,
  attributes,
  highestPhysical,
  highestMental,
  highestSocial,
  highestSpecial,
}: PcDocument): number => {
  const base =
    + dodgeScaling.base
    + dodgeScaling.bonus
    + dodgeScaling.level * level
  
    const physical =
    + dodgeScaling.Agility    * attributes.Agility   .value
    + dodgeScaling.Fortitude  * attributes.Fortitude .value
    + dodgeScaling.Might      * attributes.Might     .value
    
  const mental =
    + dodgeScaling.Learning   * attributes.Learning  .value
    + dodgeScaling.Logic      * attributes.Logic     .value
    + dodgeScaling.Perception * attributes.Perception.value
    + dodgeScaling.Will       * attributes.Will      .value
    
  const social =
    + dodgeScaling.Deception  * attributes.Deception .value
    + dodgeScaling.Persuasion * attributes.Persuasion.value
    + dodgeScaling.Presence   * attributes.Presence  .value

  const special =
    + dodgeScaling.Alteration * attributes.Alteration.value
    + dodgeScaling.Creation   * attributes.Creation  .value
    + dodgeScaling.Energy     * attributes.Energy    .value
    + dodgeScaling.Entropy    * attributes.Entropy   .value
    + dodgeScaling.Influence  * attributes.Influence .value
    + dodgeScaling.Movement   * attributes.Movement  .value
    + dodgeScaling.Prescience * attributes.Prescience.value
    + dodgeScaling.Protection * attributes.Protection.value

  const highest =
    + dodgeScaling.highestPhysical * highestPhysical .value
    + dodgeScaling.highestMental   * highestMental   .value
    + dodgeScaling.highestSocial   * highestSocial   .value
    + dodgeScaling.highestSpecial  * highestSpecial  .value
    
  const maxMp = base + physical + mental + social + special + highest
  //! TODO: this should be Math.floor for future versions
  return Math.round(maxMp)
}