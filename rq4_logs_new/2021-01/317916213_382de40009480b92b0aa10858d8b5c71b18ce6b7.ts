import PlayerUserModel from "Models/PlayerUser"
import PcModel from "Models/PlayerCharacter"

import removeCharacter from "./removeCharacter"

describe(".removeCharacter()", () => {
  const mockUser = () => PlayerUserModel.createUser({ userId: "123456789", username: "usernameTest" })

  it('allows removing a character', async () => {
    const user = mockUser()

    // add character
    const allor = PcModel.createCharacter({ name: "Allor" })
    user.addCharacter(allor)
    
    // check that the character was removed from user
    expect(user.characters[0].name).toBe("Allor")
    expect(user.characters.length).toBe(1)
    expect(user.activeCharIndex).toBe(0)

    // remove character
    removeCharacter.call(user, allor)
    // check that the character was removed from user
    expect(user.characters.length).toBe(0)
    expect(user.activeCharIndex).toBe(undefined)
  })

  it('resets the characterIndex', () => {
    const user = mockUser()

    // add character
    const allor = PcModel.createCharacter({ name: "Allor" })
    const horu = PcModel.createCharacter({ name: "Horu" })
    user.addCharacter(allor)
    user.addCharacter(horu)
    
    // check that the character was removed from user
    expect(user.characters.length).toBe(2)
    expect(user.characters[0].name).toBe("Allor")
    expect(user.characters[1].name).toBe("Horu")
    expect(user.activeCharIndex).toBe(0)

    // remove character
    removeCharacter.call(user, allor)
    // check that the character was removed from user
    expect(user.characters.length).toBe(1)
    expect(user.activeCharIndex).toBe(0)

    // remove character
    removeCharacter.call(user, horu)
    // check that the character was removed from user
    expect(user.characters.length).toBe(0)
    expect(user.activeCharIndex).toBe(undefined)
  })
})