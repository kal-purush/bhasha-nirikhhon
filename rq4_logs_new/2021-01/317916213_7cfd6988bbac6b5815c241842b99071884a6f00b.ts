import PlayerUserModel from "Models/PlayerUser"
import PcModel from "Models/PlayerCharacter"

import getCharacter from "./getCharacter"

describe(".getCharacter()", () => {
  const mockUser = () => PlayerUserModel.createUser({ userId: "123456789", username: "usernameTest" })

  it('allows getting a character by fullname', async () => {
    const user = mockUser()
    // add character
    user.addCharacter(PcModel.createCharacter({ name: "Allor Aglon" }))
    
    // const allor = user.getCharacter("Allor Aglon")
    const allor = getCharacter.call(user, "Allor Aglon")

    // check that the character was added to user
    expect(allor?.name).toBe("Allor Aglon")
  })
  
  it(`doesn't care about CaSe`, async () => {
    const user = mockUser()
    // add character
    user.addCharacter(PcModel.createCharacter({ name: "Allor Aglon" }))
    
    const allor = getCharacter.call(user, "ALLOR aglon")

    // check that the character was added to user
    expect(allor?.name).toBe("Allor Aglon")
  })
  
  it(`works with substrings of the full name`, async () => {
    const user = mockUser()
    // add character
    user.addCharacter(PcModel.createCharacter({ name: "Allor Aglon" }))
    
    const allor1 = getCharacter.call(user, "allor")
    const allor2 = getCharacter.call(user, "aglon")

    // check that the character was added to user
    expect(allor1?.name).toBe("Allor Aglon")
    expect(allor2?.name).toBe("Allor Aglon")
  })
})