'''
kata : https://www.codewars.com/kata/59727ff285281a44e3000011
'''

def band_name_generator(name):
    if name[0].lower() != name[-1].lower():
        return 'The {}'.format(name.capitalize())
        
    return '{}{}'.format(name.capitalize(), name[1:].lower())