import { Player } from '../entities/player';
import { Property } from '../entities/property';
import PlayersFunctions from './players-functions';

export default class GameFunctions {
  static newRound(player: Player, board: Property[]) {
    PlayersFunctions.getNewPlayerPosition(player);
    const currentProperty = board[player.currentPosition - 1];
    if (
      this.canBuy(currentProperty, player) &&
      PlayersFunctions.playerDecision(player, currentProperty)
    ) {
      this.buyProperty(player, currentProperty);
    } else if (currentProperty.owner) {
      this.collectRent(currentProperty, player);
      this.checkDefeat(player, board);
    }
  }

  static canBuy(currentProperty: Property, player: Player): boolean {
    return (
      !currentProperty.owner && currentProperty.saleValue <= player.balance
    );
  }

  static buyProperty(player: Player, property: Property): void {
    player.balance -= property.saleValue;
    property.owner = player;
  }

  static collectRent(currentProperty: Property, player: Player) {
    currentProperty.owner.balance += currentProperty.rentValue;
    player.balance -= currentProperty.rentValue;
  }

  static checkDefeat(player: Player, board: Property[]) {
    if (player.balance < 0) {
      for (const property of board.filter(
        (property) => property.owner == player,
      )) {
        property.owner = null;
      }
    }
  }
}