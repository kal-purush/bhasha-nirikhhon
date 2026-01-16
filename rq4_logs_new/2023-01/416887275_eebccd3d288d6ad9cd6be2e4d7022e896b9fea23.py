"""
This problem was asked by Facebook.

On a mysterious island there are creatures known as Quxes which come in three colors: red, green, and blue. One power of the Qux is that if two of them are standing next to each other, they can transform into a single creature of the third color.

Given N Quxes standing in a line, determine the smallest number of them remaining after any possible sequence of such transformations.

For example, given the input ['R', 'G', 'B', 'G', 'B'], it is possible to end up with a single Qux through the following steps:

        Arrangement       |   Change
----------------------------------------
['R', 'G', 'B', 'G', 'B'] | (R, G) -> B
['B', 'B', 'G', 'B']      | (B, G) -> R
['B', 'R', 'B']           | (R, B) -> G
['B', 'G']                | (B, G) -> R
['R']                     |

        Arrangement       |   Change
----------------------------------------
['R', 'G', 'B', 'G', 'B'] | (R, G) -> B
['R', 'B', 'G', 'R']      | (B, G) -> R
['R', 'B', 'B']           | (R, B) -> G
['B', 'G']                | (B, G) -> R
['R']                     |


"""

def quxes(arr: list):
    opt=set(arr)
    print(f'options::{opt}')
    i=0
    j=1+i
    while len(set(arr)) > 1:
        curr=set([arr[i], arr[j]])
        if len(curr)>1:
            trans=opt.symmetric_difference(curr)
            trans=list(trans)
            arr.pop(i)
            arr.pop(i)
            arr.insert(j,trans[0])
            print(f'arr:{arr}')
    else:
        print(f'{len(arr)}')
    # for i, q in enumerate(arr, 1):
    #     j=i-1
    #     curr=set([arr[j], q])

    #     if len(curr)>1:
    #         trans=opt.symmetric_difference(curr)

test_l=['R', 'G', 'B', 'G', 'B']
quxes(test_l)