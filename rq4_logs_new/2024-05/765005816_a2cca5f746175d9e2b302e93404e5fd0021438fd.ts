import { BaseDTO } from "../base.dto";

export interface BattleStatsDTO extends BaseDTO {
  id: string;
  ASSISTS: number;
  CHAMPIONS_KILLED: number;
  GOLD_EARNED: number;
  LEVEL: number;
  MINIONS_KILLED: number;
  NEUTRAL_MINIONS_KILLED: number;
  NEUTRAL_MINIONS_KILLED_ENEMY_JUNGLE: number;
  NEUTRAL_MINIONS_KILLED_YOUR_JUNGLE: number;
  NUM_DEATHS: number;
  TOTAL_DAMAGE_DEALT: number; //적에게 가한 피해량
  TOTAL_DAMAGE_DEALT_TO_CHAMPIONS: number; //챔피언에게 가한 피해량
  TOTAL_DAMAGE_DEALT_TO_OBJECTIVES: number; //목표물에게 가한 피해량
  TOTAL_DAMAGE_DEALT_TO_TURRETS: number; //포탑에 가한 피해량
  VISION_SCORE: number; //시야점수
  //룬정보
  PERK0: number; // ex) 정복자 8010
  PERK1: number; // ex) 승전보 9111
  PERK2: number; // ex) 전설 : 강인함 9105
  PERK3: number; // ex) 최후의 저항 8299
  PERK4: number; // ex) 뼈 방패 8473
  PERK5: number; // ex) 소생 8453
}