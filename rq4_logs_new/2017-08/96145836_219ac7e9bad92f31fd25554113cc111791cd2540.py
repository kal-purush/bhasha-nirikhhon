from numbers import Number

from characters import Character
from stats import RegenStat


class Effect:
    def __init__(self, name, obj_affected, stat_affected, attribute_affected, how_effects, effect_amount,
                 effect_duration):
        self.name = name
        if isinstance(obj_affected, Character):
            if not isinstance(stat_affected, RegenStat):
                raise TypeError('Not A Valid Stat to Modify')
        else:
            raise TypeError('Not A Valid obj to Modify')
        self.obj_affected = obj_affected
        if attribute_affected not in {'Current', 'Max', 'Rate'}:
            raise AssertionError('Not A Valid Attribute to Effect')
        self.attribute_affected = attribute_affected
        self.stat_affected = stat_affected
        if how_effects not in {'Set', 'Add'}:
            raise AssertionError('Not a Valid effect type')
        self.how_effects = how_effects
        if not isinstance(effect_amount, Number):
            effect_amount = float(effect_amount)
        self.effect_amount = effect_amount
        self.old_amount = 0
        if not isinstance(effect_duration, Number):
            effect_duration = float(effect_duration)
        self.effect_duration = effect_duration
        self.effect_used = 0
        self.active = False

    def activate(self):
        """Activates the effect."""
        self.effect_used = 0
        self.old_amount = self.obj_affected[self.stat_affected][self.attribute_affected]
        if self.how_effects == 'Set':
            self.obj_affected[self.stat_affected][self.attribute_affected] = self.effect_duration
        elif self.how_effects == 'Add':
            self.obj_affected[self.stat_affected][self.attribute_affected] += self.effect_duration
        self.active = True

    def deactivate(self):
        """Deactivates the effect only if duration is used up else increments used."""
        if self.effect_used < self.effect_duration:
            self.effect_used += 1
        else:
            if self.how_effects == 'Set':
                self.obj_affected[self.stat_affected][self.attribute_affected] = self.old_amount
            elif self.how_effects == 'Add':
                self.obj_affected[self.stat_affected][self.attribute_affected] -= self.effect_duration
            self.active = True


class ManaRegenBoost(Effect):
    def __init__(self, name, obj_affected, stat_affected, attribute_affected, how_effects, effect_amount,
                 effect_duration):
        super().__init__(name, obj_affected, stat_affected, attribute_affected, how_effects, effect_amount,
                         effect_duration)