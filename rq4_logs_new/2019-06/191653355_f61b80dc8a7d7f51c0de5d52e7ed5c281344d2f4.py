"""
Just demonstrate all coloring options available
when printing to a linux terminal
"""

for c in range(127):
    print(f"\033[0;{c}mTHIS IS MODIFIER #{str(c).rjust(3, '0')} "
          f"\033[0;{c};2m(and its dimmed version)"
          f"\033[0m")