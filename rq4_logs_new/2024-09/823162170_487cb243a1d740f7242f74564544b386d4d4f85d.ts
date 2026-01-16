export type NoArgCallBack = () => void;
// TODO merge with src/utils/currentInstance.ts: type InstanceId="main"|"recharge"|"soloSprint"|"fuegoCircuit"|"demoDerby"|"dragRace"|"scrapyard"
export type RaceType = "circuit" | "derby" | "lobby" | "dragrace" | "solosprint" | "grandprix";
export type CarDataRaceType = RaceType;

export type UpdateType = "server" | "local";

export type ProjectileConfig = {
  enabled: boolean;
  speed: number;
  lifeInSeconds: number;
  maxReloadAmount: number;
  reloadTimeSeconds: number;
  shootCooldownTimeMs: number;
};

export type LeaderBoardConfig = {
  maxNames: number;
  fontSize: number;
  positionY: string | number;
  height: string | number;
};