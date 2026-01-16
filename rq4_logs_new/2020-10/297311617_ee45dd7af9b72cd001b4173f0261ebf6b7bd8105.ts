import { Input } from "../../src/input/input"
import { Player } from "../../src/player/player"

jest.mock("../../src/player/player", () => ({
 Player: jest.fn().mockImplementation(() => {
    return {
      step: jest.fn(),
      left: jest.fn(),
      right: jest.fn(),
      reset: jest.fn(),
    }
  })
}));

describe ("Input responses", function() {

  describe ("keyPress - step forward", function() {
    it( "calls player.step", function() {
      var mockedPlayer = new Player 
      var input = new Input(mockedPlayer)

      var key = "ArrowUp"
      input.keyResponse(key)
      expect(input.player.step).toBeCalledTimes(1)
    })
  })

  describe ("keyPress - turn left", function() {
    it( "calls player.left", function() {
      var mockedPlayer = new Player 
      var input = new Input(mockedPlayer)

      var key = "ArrowLeft"
      input.keyResponse(key)
      expect(input.player.left).toBeCalledTimes(1)
    })
  })

  describe ("keyPress - turn right", function() {
    it( "calls player.right", function() {
      var mockedPlayer = new Player 
      var input = new Input(mockedPlayer)

      var key = "ArrowRight"
      input.keyResponse(key)
      expect(input.player.right).toBeCalledTimes(1)
    })
  })
})