import functools

def A():
  def B(x):
    return x + 1
  return B

def C(func):
  def D(*args, **kwargs):
    x = func(*args, **kwargs)
    x += '!!!'
    print(x)
  return D

@C
def E():
  return 'Hello, World!'

def F(a, b, c):
  for f in [a, b, c]: f()

def main():
  # I.
  x = 1
  print(A()(x))

  # II.
  E()

  # III.
  F(lambda: print('A!'),
    lambda: print('B!'),
    lambda: print('C!'))

if __name__ == '__main__':
  main()