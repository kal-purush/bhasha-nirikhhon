import axios, { Axios, AxiosResponse } from "axios";
import { BattleDTO } from "../common/DTOs/battle/battle.dto";
import { BattlePlayerDTO } from "../common/DTOs/battle/battle_player.dto";
import { ResponseDTO } from "../common/DTOs/response.dto";

const baseUrl = `${process.env.SERVER_URL}/battle`;

/**
 * Battle 내전 결과 저장
 * @param data
 */
export const recordBattle = async (
  data: any
): Promise<AxiosResponse<ResponseDTO<BattleDTO[]>>> => {
  let url = `${baseUrl}`;

  const battleResultData: BattleDTO = {
    battleId: data.gameId,
    battleMode: data.gameMode,
    battleLength: data.gameLength,
    teamA: {
      isWinning: data.teams[0].isWinningTeam,
      guildName: "롤파이트",
      player1: createPlayerDTO(data.teams[0]?.players[0]),
      player2: createPlayerDTO(data.teams[0]?.players[1]),
      player3: createPlayerDTO(data.teams[0]?.players[2]),
      player4: createPlayerDTO(data.teams[0]?.players[3]),
      player5: createPlayerDTO(data.teams[0]?.players[4]),
    },
    teamB: {
      isWinning: data.teams[1].isWinningTeam,
      guildName: "hank",
      player1: createPlayerDTO(data.teams[1]?.players[0]),
      player2: createPlayerDTO(data.teams[1]?.players[1]),
      player3: createPlayerDTO(data.teams[1]?.players[2]),
      player4: createPlayerDTO(data.teams[1]?.players[3]),
      player5: createPlayerDTO(data.teams[1]?.players[4]),
    },
  };

  return await axios.post(url, battleResultData);
};

//=======================================================================//

function createPlayerDTO(playerData: any): BattlePlayerDTO | null {
  if (!playerData) return null;

  const itemsString = playerData.items ? playerData.items.join(", ") : "";
  return {
    championId: playerData.championId ?? null,
    summonerName: playerData.summonerName ?? null,
    detectedTeamPosition: playerData.detectedTeamPosition ?? null,
    items: itemsString ?? null,
    spell1Id: playerData.spell1Id ?? null,
    spell2Id: playerData.spell2Id ?? null,
    killed: playerData.stats?.CHAMPIONS_KILLED ?? null,
    deaths: playerData.stats?.NUM_DEATHS ?? null,
    assists: playerData.stats?.ASSISTS ?? null,
    gold: playerData.stats?.GOLD_EARNED ?? null,
    level: playerData.stats?.LEVEL ?? null,
    minionsKilled:
      playerData.stats?.MINIONS_KILLED +
        playerData.stats?.NEUTRAL_MINIONS_KILLED ?? null,
    totalDamage: playerData.stats?.TOTAL_DAMAGE_DEALT ?? null,
    totalChampionsDamage:
      playerData.stats?.TOTAL_DAMAGE_DEALT_TO_CHAMPIONS ?? null,
    visionScore: playerData.stats?.VISION_SCORE ?? null,
    perk0: playerData.stats?.PERK0,
    perk1: playerData.stats?.PERK1,
    perk2: playerData.stats?.PERK2,
    perk3: playerData.stats?.PERK3,
    perk4: playerData.stats?.PERK4,
    perk5: playerData.stats?.PERK5,
  };
}