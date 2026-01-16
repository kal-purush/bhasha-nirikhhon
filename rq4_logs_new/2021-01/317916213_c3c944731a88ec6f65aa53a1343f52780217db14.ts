
import PcModel from ".."

import rollInitiative from "./rollInitiative"

describe('PlayerCharacter.rollInitiative()', () => {
  const mockPc = () => PcModel.createCharacter({
    name: "testName",
  })

  it('works', () => {
    const mockedPc = mockPc()
    mockedPc.attributes.Agility.value = 1
    mockedPc.attributes.Agility.bonus = 3    
    expect(rollInitiative.call(mockedPc).diceArgs).toEqual(expect.objectContaining({ dieMax: 20, bonus: 4 }))
  })
  it('works with aditional args', () => {
    const mockedPc = mockPc()
    mockedPc.attributes.Agility.value = 2
    mockedPc.attributes.Agility.bonus = 3    
    expect(rollInitiative.call(mockedPc, { advantage: -1, bonus: 2 }).diceArgs).toEqual(expect.objectContaining({ dieMax: 20, bonus: 7, advantage: -1 }))
  })
})