from datetime import datetime
from pytz import timezone

class Library(object):

  users = {} #format: {user id: user object}

  def __init__(self):
    subs = open('borrowers.txt')
    for line in subs:
      chunks = line.split(',')

      user_details = chunks[0].split(':')
      self.users[user_details[0]] = User(user_details[1])

      for device in chunks[1:]:
        device = device.split(':')
        self.borrow_device(user_details[0], user_details[1], device[0], device[1])

  def borrow_device(self, user_id, user_name, device, date = None):
    if user_id not in self.users:
      self.users[user_id] = User(user_name)

    self.users[user_id].borrow_device(device, date)

  def return_device(self, user_id, device):
    if user_id not in self.users:
      return -1

    if self.users[user_id].return_device(device) == -2:
      return -2

  def all_borrowed_devices(self):
    out = ''

    for user_id, user in self.users.items():
      if user.count() > 0:
        out += '<@{}> has borrowed:\n{}\n'.format(user_id, user.list_devices())

    return out

  def count(self):
    total = 0

    for user in self.users.values():
      total += user.count()

    return total

  def write_library(self):
    f = open('borrowers.txt', 'w')

    for user_id, user in self.users.items():
      if user.count() > 0:
        f.write('{}:{}\n'.format(user_id, user.write_user()))

class User(object):

  name = ''
  borrowed_devices = {}

  def __init__(self, name):
    self.name = name
    self.borrowed_devices = {}

  def borrow_device(self, device, date = None):
    if device in self.borrowed_devices:
      return -1

    if date == None:
      self.borrowed_devices[device] = datetime.now(timezone('Australia/Sydney')).date()
    else:
      self.borrowed_devices[device] = datetime.strptime(date.strip(), '%Y/%m/%d').date() #format is yyyy/mm/dd, e.g. 2016/04/15, for April 15th, 2016.

  def return_device(self, device):
    if device not in self.borrowed_devices:
      return -2

    self.borrowed_devices.pop(device)

  def list_devices(self):
    out = ''
    current_date = datetime.now(timezone('Australia/Sydney')).date()

    for device, date in self.borrowed_devices.items():
      out += '    {}, borrowed {} days ago.\n'.format(device, (current_date-date).days)

    return out

  def count(self):
    return len(self.borrowed_devices)

  def write_user(self):
    out = '{}'.format(self.name)

    for device, date in self.borrowed_devices.items():
      out += ',{}:{}'.format(device, datetime.strftime(date, '%Y/%m/%d'))

    return out