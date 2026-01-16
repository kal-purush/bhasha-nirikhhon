import { ErrorCode, GPError } from '../../errors';
import { EventType, ObserverPublisher } from '../../events';
import { FreeAction, Income, Player } from '../../interfaces';

type Exchange = {
  cost: Partial<Income>;
  value: Partial<Income>;
};

export class FreeActions {
  private static Exchanges: Record<FreeAction, Exchange> = {
    [FreeAction.GenerateCredit]: {
      cost: { powerCharge: 1 },
      value: { credits: 1 },
    },
    [FreeAction.GenerateKnowledge]: {
      cost: { powerCharge: 4 },
      value: { knowledge: 1 },
    },
    [FreeAction.GenerateOre]: {
      cost: { powerCharge: 3 },
      value: { ore: 1 },
    },
    [FreeAction.GenerateQIC]: {
      cost: { powerCharge: 4 },
      value: { qic: 1 },
    },
    [FreeAction.TradeKnowledgeForCredit]: {
      cost: { knowledge: 1 },
      value: { credits: 1 },
    },
    [FreeAction.TradeOreForCredit]: {
      cost: { ore: 1 },
      value: { credits: 1 },
    },
    [FreeAction.TradeOreForPowerNode]: {
      cost: { ore: 1 },
      value: { powerNodes: 1 },
    },
    [FreeAction.TradeQICForOre]: {
      cost: { qic: 1 },
      value: { ore: 1 },
    },
  } as const;

  private static validateCost(player: Player, cost: Partial<Income>): void {
    if (cost.knowledge && player.resources.knowledge < cost.knowledge) {
      throw new GPError(
        ErrorCode.InsufficientKnowledge,
        `Insufficient knowledge. You need ${cost.knowledge} knowledge to perform this action.`,
      );
    }

    if (cost.ore && player.resources.ore < cost.ore) {
      throw new GPError(
        ErrorCode.InsufficientOre,
        `Insufficient ore. You need ${cost.ore} ore to perform this action.`,
      );
    }

    if (cost.qic && player.resources.qic < cost.qic) {
      throw new GPError(
        ErrorCode.InsufficientQICs,
        `Insufficient QICs. You need ${cost.qic} QICs to perform this action.`,
      );
    }

    // TODO: Brainstone!! Need to account for this.
    if (cost.powerCharge && player.powerCycle.level3 < cost.powerCharge) {
      throw new GPError(
        ErrorCode.InsufficientPower,
        `Insufficient power. You need ${player.powerCycle.level3} charged power nodes to perform this action.`,
      );
    }
  }

  static performFreeAction(
    player: Player,
    events: ObserverPublisher,
    action: FreeAction,
  ): void {
    const { cost, value } = FreeActions.Exchanges[action];

    // 1. Validate that the player has the resources.
    FreeActions.validateCost(player, cost);

    // 2. Charge the player the cost.
    // TODO: Update PlayerBase so that it can deduct/discharge power nodes.
    events.publish({
      type: EventType.ResourcesSpent,
      player,
      resources: cost,
    });

    // 3. Grant the player the value of the exchange.
    events.publish({
      type: EventType.IncomeGained,
      player,
      income: value,
    });
  }
}