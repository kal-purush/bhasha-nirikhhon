# Understanding lists
def Lists():
  month = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'June']
  prices = [10, 30, 50, 75, 100, 30]
  month.append('July')
  prices.append(150)
  # Insert a new value '12' at the 0th position in List
  prices.insert(0, 12)
  print('Number of times 30 repeats in prices list ', month.count(30))
  # Need to remove month March from initial list
  month.pop(2)
  monthlistlen = len(month)
  print('Contents of list month are: ', month)
  print('Contents of list prices are:', prices)
  print('The length of list month is :', monthlistlen)

  # Need to understand the first appearance of i.e position number of "Apr" in list month
  print(month)
  print('the position of apr in list month is at', month.index('Apr'))

Lists()