from random import randint

class Character:
  def __init__(self):
    self.name = ""
    self.health = 10
    self.health_max = 10
    self.wealth=10
  def do_damage(self, enemy):
    damage = min(
        max(randint(0, self.health) - randint(0, enemy.health), 0),
        enemy.health)
    enemy.health = enemy.health - damage
    if damage == 0: print("%s evades %s's attack." % (enemy.name, self.name))
    else: print ("%s hurts %s!" % (self.name, enemy.name))
    return enemy.health <= 0