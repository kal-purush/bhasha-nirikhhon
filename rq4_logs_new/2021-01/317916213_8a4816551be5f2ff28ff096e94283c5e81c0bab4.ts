import mockAttributes from "Utils/mockAttributes"
import PcModel, { Pc } from ".."

import rollAtk from "./rollAtk"

describe('PlayerCharacter.rollAtk()', () => {
  const mockPc = () => {
    const props: Pc = {
      name: "testName",
      attributes: mockAttributes()
    }
    return new PcModel(props)
  }

  it('works', () => {
    const mockedPc = mockPc()
    mockedPc.attributes.Agility.value = 2
    mockedPc.attributes.Agility.bonus = 1
    expect(rollAtk.call(mockedPc, "Agility").diceArgs).toEqual(expect.objectContaining({ dieAmmount: 1, dieMax: 20, bonus: 6 }))
  })

  it('works with bonus arguments', () => {
    const mockedPc = mockPc()
    mockedPc.attributes.Entropy.value = 1
    mockedPc.attributes.Entropy.bonus = -3
    expect(rollAtk.call(mockedPc, "Entropy", { advantage: 2, bonus: -3 }).diceArgs).toEqual(expect.objectContaining({ dieAmmount: 1, dieMax: 20, advantage: 2, bonus: -7 }))
  })
})