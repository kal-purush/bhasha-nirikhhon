import { describe, expect, it } from '@jest/globals';

import { ensureAttributes } from './config';
import { EnsureAttributesParams } from './interface';

describe('ensureAttributes', () => {
  it('should return the expected attributes when moveByOrder is present', () => {
    const params = {
      pp: 10,
      type: 'fire',
      power: 50,
      target: 'enemy',
      priority: 1,
      accuracy: 95,
      damage_class: 'physical',
      effect_chance: 30,
      moveByOrder: {
        pp: 10,
        type: { name: 'fire', url: 'fire_url' },
        power: 50,
        target: { name: 'enemy', url: 'enemy_url' },
        priority: 1,
        accuracy: 95,
        damage_class: { name: 'physical', url: 'damage_class_url' },
        effect_chance: 30,
        effect_entries: [
          {
            effect: 'Burns the target.',
            short_effect: 'Burns target occasionally.',
          },
        ],
        name: 'ember',
        learned_by_pokemon: [],
      },
    };

    const result = ensureAttributes(params);
    expect(result).toEqual({
      pp: 10,
      type: 'fire',
      power: 50,
      target: 'enemy',
      effect: 'Burns the target.',
      short_effect: 'Burns target occasionally.',
      priority: 1,
      accuracy: 95,
      damage_class: 'physical',
      effect_chance: 30,
    });
  });

  it('should return the expected attributes when moveByOrder is absent', () => {
    const params: EnsureAttributesParams = {
      pp: 15,
      type: 'water',
      power: 40,
      target: 'ally',
      priority: 0,
      accuracy: 90,
      damage_class: 'special',
      effect_chance: 20,
    };

    const result = ensureAttributes(params);

    expect(result).toEqual({
      pp: 15,
      type: 'water',
      power: 40,
      target: 'ally',
      effect: undefined,
      short_effect: undefined,
      priority: 0,
      accuracy: 90,
      damage_class: 'special',
      effect_chance: 20,
    });
  });

  it('should return undefined values for missing attributes', () => {
    const params: EnsureAttributesParams = {};

    const result = ensureAttributes(params);

    expect(result).toEqual({
      pp: undefined,
      type: undefined,
      power: undefined,
      target: undefined,
      effect: undefined,
      short_effect: undefined,
      priority: undefined,
      accuracy: undefined,
      damage_class: undefined,
      effect_chance: undefined,
    });
  });
});