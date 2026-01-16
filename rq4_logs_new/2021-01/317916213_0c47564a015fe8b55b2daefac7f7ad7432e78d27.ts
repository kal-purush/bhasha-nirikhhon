import { useDbConnection } from "@discord-bot/mongo"
import { mockMessage } from "@discord-bot/discord-mock"
import PcModel from "Models/PlayerCharacter"
import PlayerUserModel from "Models/PlayerUser"
import { mockDmMessage, mockPlayerMessage } from "Utils/Mock/mockMessage"

import { test, execute } from "./help"

describe("Command: help", () => {
  useDbConnection("Command_help")

  describe(".test", () => {
    it("works", () => {
      expect(`!help`).toMatch(test)
      expect(`!wrong`).not.toMatch(test)
    })
  })

  describe(".execute", () => {
    it('works', async () => {
      const [message] = mockPlayerMessage()
      message.content = `!help`
      await execute(message, test.exec(message.content)!)
      expect(message.channel.send).toBeCalledTimes(1)
      expect(message.channel.send).toBeCalledWith(
        `The available commands are:`
        + `\n\t"!help\" -> recursão!`
        + `\n\t"!createRoles\" -> Criar as roles de player e Mestre no servidor`
        + `\n\t"!createChar\" -> Criar um novo personagem`
        + `\n\t"!setActiveChar (charName)\" -> Mudar o personagem ativo`
        + `\n\t"!listChars\" -> Listar todos os personagens de um player`
        + `\n\t"!deleteChar (charName)\" -> Deletar um personagem`
        + `\n\t"!viewBio\" -> Ver as informações de um personagem`
        + `\n\t"!viewStats\" -> Ver as informações de um personagem (resumido)`
        + `\n\t"!setAtkAttb\" -> Modificar o attributo padrão de ataque de um personagem`
        + `\n\t"!setAttb\" -> Modificar o valor/bonus de um attributo de um personagem`
        + `\n\t"!setLvl\" -> Modificar o nível de um personagem`
        + `\n\t"!set (hp/mp) scaling\" -> Modificar como o hp/mp de um personagem escalam com os seus atributos`
        + `\n\t"!set (guard/dodge) Scaling\" -> Modificar como o guard/dodge de um personagem escalam com os seus atributos`
        + `\n\t"!(attbName) [bonus] [vantagem+x]\" -> Rolar um atributo`
        + `\n\t"!atk [(attbName)] [bonus] [vantagem+x]\" -> Rolar um ataque`
        + `\n\t"!dmg [(attbName)] [bonus] [vantagem+x]\" -> Rolar dano`
        + `\n\t"!initiative [bonus] [vantagem+x]\" -> Rolar iniciativa`
        + `\n\t"!takeDmg (numero)\" -> Tomar dano usando as defesas do personagem`
        + `\n\t"!dmgTable [numero]\" -> Checar a tabela de dano ou rolar um attributo de dano`
        + `\n\t"!(hp|mp) +/- (numero)\" -> Para mudar o hp/mp do personagem`
        + `\n\t"!shortRest\" -> Fazer um descanso curto`
        + `\n\t"!longRest\" -> Fazer um descanso longo`
      )
    })
  })
})