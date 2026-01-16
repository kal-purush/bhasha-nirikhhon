import { z } from 'zod';

import { FactionType } from '../factions';
import {
  FreeAction,
  GameState,
  PlanetType,
  RoundBoosterBonusType,
  RoundBoosterPassBonusDiscriminator,
  StructureType,
} from '../interfaces';
import { BuildFirstMinesPass } from '../states/build-first-mines-state';

const ResourceSchema = z.number().int().min(0);
const ResearchProgressSchema = z.number().int().min(0).max(5);

const RoundBoosterActionBonusSchema = z.object({
  type: z.literal(RoundBoosterBonusType.Action),
  action: z.nativeEnum(FreeAction),
});
const RoundBoosterIncomeBonusSchema = z.object({
  type: z.literal(RoundBoosterBonusType.Income),
  income: z
    .object({
      credits: z.number().int(),
      ore: z.number().int(),
      knowledge: z.number().int(),
      qic: z.number().int(),
      powerNodes: z.number().int(),
      chargePower: z.number().int(),
    })
    .partial(),
});
const RoundBoosterPassBonusSchema = z.object({
  type: z.literal(RoundBoosterBonusType.BonusOnPass),
  discriminator: z.nativeEnum(RoundBoosterPassBonusDiscriminator),
  vp: z.number().int().positive(),
});
const RoundBoosterBonusSchema = z.discriminatedUnion('type', [
  RoundBoosterActionBonusSchema,
  RoundBoosterPassBonusSchema,
  RoundBoosterIncomeBonusSchema,
]);
const RoundBoosterSchema = z.object({
  id: z.number().int().min(0),
  a: RoundBoosterBonusSchema,
  b: RoundBoosterBonusSchema,
});

const ActionPhaseStateSchema = z.object({
  type: z.literal(GameState.ActionPhase),
  player: z.number().int(),
});

const BuildFirstMinesStateSchema = z.object({
  type: z.literal(GameState.BuildFirstMines),
  pass: z.nativeEnum(BuildFirstMinesPass),
  player: z.number().int(),
});

const ChooseFirstRoundBoostersStateSchema = z.object({
  type: z.literal(GameState.ChooseFirstRoundBoosters),
  player: z.number().int(),
});

// No variablity... just the state type.
const CleanupPhaseStateSchema = z.object({
  type: z.literal(GameState.CleanupPhase),
});
const GaiaPhaseStateSchema = z.object({
  type: z.literal(GameState.GaiaPhase),
});
const GameEndedStateSchema = z.object({
  type: z.literal(GameState.GameEnded),
});
const GameNotStartedStateSchema = z.object({
  type: z.literal(GameState.GameNotStarted),
});
const IncomePhaseStateSchema = z.object({
  type: z.literal(GameState.IncomePhase),
});

const StateSchema = z.discriminatedUnion('type', [
  ActionPhaseStateSchema,
  BuildFirstMinesStateSchema,
  CleanupPhaseStateSchema,
  ChooseFirstRoundBoostersStateSchema,
  GaiaPhaseStateSchema,
  GameEndedStateSchema,
  GameNotStartedStateSchema,
  IncomePhaseStateSchema,
]);
export type SerializedState = z.infer<typeof StateSchema>;

const MapHexSchema = z.object({
  location: z.object({
    q: z.number().int(),
    r: z.number().int(),
  }),
  planet: z
    .object({
      type: z.nativeEnum(PlanetType),
      player: z.number().int().min(0).max(3).optional(),
      structure: z.nativeEnum(StructureType).optional(),
      hasLantidMine: z.boolean().optional(),
    })
    .optional(),
  hasIvitsStation: z.boolean().optional(),
});

const PlayerSchema = z.object({
  id: z.string(),
  name: z.string(),
  faction: z.nativeEnum(FactionType),

  powerCycle: z.object({
    gaia: ResourceSchema,
    l1: ResourceSchema,
    l2: ResourceSchema,
    l3: ResourceSchema,
  }),

  resources: z.object({
    credits: ResourceSchema,
    knowledge: ResourceSchema,
    ore: ResourceSchema,
    qic: ResourceSchema,
  }),

  research: z.object({
    terraforming: ResearchProgressSchema,
    navigation: ResearchProgressSchema,
    ai: ResearchProgressSchema,
    gaia: ResearchProgressSchema,
    economics: ResearchProgressSchema,
    science: ResearchProgressSchema,
  }),

  roundBooster: RoundBoosterSchema.optional(),

  vp: z.number().int(),
});
export type SerializedPlayer = z.infer<typeof PlayerSchema>;

export const GameContextSchema = z.object({
  currentRound: z.number().int().min(0).max(6),
  currentPlayer: z.number().int().min(0).max(3),
  currentState: StateSchema,
  players: PlayerSchema.array().min(1).max(4),
  // .refine((players) => {
  //   // TODO: Ensure valid/unique IDs.
  //   // TODO: Validate for faction conflicts.
  // }),
  map: MapHexSchema.array(),
  roundBoosters: RoundBoosterSchema.array(),
});
export type SerializedGameContext = z.infer<typeof GameContextSchema>;