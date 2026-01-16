import { Player } from '../lib/entities/player';
import { Property } from '../lib/entities/property';
import GameFunctions from '../lib/utils/game-functions';

describe('Properties Tests', () => {
  it('should be defined', () => {
    expect(GameFunctions).toBeDefined();
  });

  it('shoud be return not can buy', async () => {
    expect(GameFunctions.canBuy({ owner: {} as Player } as Property, {} as Player)).toBeFalsy();
    expect(GameFunctions.canBuy({ saleValue: 2 } as Property, { balance: 1 } as Player)).toBeFalsy()
  })

  it('shoud be return can buy', async () => {
    expect(GameFunctions.canBuy({ saleValue: 2 } as Property, { balance: 3 } as Player)).toBeTruthy()
  })

  it('shoud buy property', async () => {
    const player = { balance: 3 } as Player;
    const property = { saleValue: 2 } as Property;
    GameFunctions.buyProperty(player, property)
    expect(player).toStrictEqual({ balance: 1 })
    expect(property.owner).toStrictEqual({ balance: 1 })
  })

  it('shoud collect rent', async () => {
    const player = { balance: 3 } as Player;
    const property = { rentValue: 2, owner: { balance: 1 } } as Property;
    GameFunctions.collectRent(property, player)
    expect(player).toStrictEqual({ balance: 1 })
    expect(property.owner).toStrictEqual({ balance: 3 })
  })

  it('shoud check defeat', async () => {
    const player = { balance: -1 } as Player;
    const property = { rentValue: 2, owner: player } as Property;
    GameFunctions.checkDefeat(player, [property])
    expect(property.owner).toBeNull()
  })
});