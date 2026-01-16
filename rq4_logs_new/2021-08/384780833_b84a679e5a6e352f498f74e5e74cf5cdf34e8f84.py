'''
Class that houses the individual data for a Steam user
'''

class PlayerData:
  def __init__(self, name, sheet):
      self._name = name
      self._teams = []
      self._sheet = sheet
      self._row = 1 #0 is the column titles

  def get_name(self):
    return self._name

  def get_teams(self):
    return self._teams

  def set_teams(self, group, team, data):
    self._teams.append((group, team, data))

  def get_worksheet(self):
    return self._sheet

  def get_row(self):
    return self._row

  def set_row(self, row):
    self._metadata.set_row(row)