import random # a module not an external libary
#Numpy is not allow as it is an external libary

def normal_dist_length():
    count = 10
    mean = 65 # mean at center
    std = 6.5 * 3 # At three Standard deviation, this account for 99.7 percent of all possible range
    low = round(mean - std) # lower range
    high = round(mean + std) # upper range
    values =  sum([random.randint(low, high) for x in range(count)])

    out = round(values/count, 2)

    # clamp outliners values from more than 1.2x to 1.15x
    if (out > 78):
        out = 74.7
    elif (out < 52):
        out = 55.2

    return out

def normal_dist_widthp():
    count = 10
    mean = 215 # mean at center
    std = 6.5 * 3 # At three Standard deviation, this account for 99.7 percent of all possible range
    low = round(mean - std) # lower range
    high = round(mean + std) # upper range
    values =  sum([random.randint(low, high) for x in range(count)])

    out = round(values/count, 2)

    # clamp outliners values from more than 1.2x to 1.15x
    if (out > 258):
        out = 247.2
    elif (out < 172):
        out = 182.7

    return out

def normal_dist_widthn():
    count = 10
    mean = 130 # mean at center
    std = 6.5 * 3 # At three Standard deviation, this account for 99.7 percent of all possible range
    low = round(mean - std) # lower range
    high = round(mean + std) # upper range
    values =  sum([random.randint(low, high) for x in range(count)])

    out = round(values/count, 2)

    # clamp outliners values from more than 1.2x to 1.15x
    if (out > 156):
        out = 149.5
    elif (out < 104):
        out = 110.5

    return out

def normal_dist_TOXEP():
    count = 10
    mean = 195 # mean at center
    std = 5 * 3 # At three Standard deviation, this account for 99.7 percent of all possible range
    low = round(mean - std) # lower range
    high = round(mean + std) # upper range
    values =  sum([random.randint(low, high) for x in range(count)])

    out = round(values/count / 100, 2) # shift decimal to left by 2

    # clamp outliners values from more than 1.2x to 1.15x
    if (out > 2.34):
        out = 2.24
    elif (out < 1.56):
        out = 1.66

    return out

def normal_dist_TOXEN():
    count = 10
    mean = 185 # mean at center
    std = 5 * 3 # At three Standard deviation, this account for 99.7 percent of all possible range
    low = round(mean - std) # lower range
    high = round(mean + std) # upper range
    values =  sum([random.randint(low, high) for x in range(count)])

    out = round(values/count / 100, 2) # shift decimal to left by 2

    # clamp outliners values from more than 1.2x to 1.15x
    if (out > 2.22):
        out = 2.13
    elif (out < 1.48):
        out = 1.57

    return out

dist = [normal_dist_length()]
dist2 = [normal_dist_widthp()]
dist3 = [normal_dist_widthn()]
dist4 = [normal_dist_TOXEP()]
dist5 = [normal_dist_TOXEN()]
print(dist)
print(dist2)
print(dist3)
print(dist4)
print(dist5)

# Creation of multiple files

for i in range(0,8):
    with open(f'ring_oscillator_{i}.cir', 'w') as test:
        test.write('* run_inverter_nand\n.include inverter.cir\n.include nand.cir\n\n' + 
            '* subckt for ring_oscillator\n' + '.subckt ring_oscillator_' + str(i) + ' va in vdd vss\n\n' +
            '* nand instance\n'
            'x0 va vb in mid vdd vss nand\n\n' +
            '* inverter instance\n' +
            'x1 in out vdd vss inverter ' + 'tplv=' + str(normal_dist_length()) + 'n' + ' tpwv=' + str(normal_dist_widthp()) + 'n' + ' tnln=' + str(normal_dist_length()) + 'n' +
            ' tnwm=' + str(normal_dist_length()) + 'n' + ' tpotv=' + str(normal_dist_TOXEP()) + 'n' + ' tnotv=' + str(normal_dist_TOXEN()) + 'n\n' +

            'x2 out out2 vdd vss inverter '  + 'tplv=' + str(normal_dist_length()) + 'n' + ' tpwv=' + str(normal_dist_widthp()) + 'n' + ' tnln=' + str(normal_dist_length()) + 'n' +
            ' tnwm=' + str(normal_dist_length()) + 'n' + ' tpotv=' + str(normal_dist_TOXEP()) + 'n' + ' tnotv=' + str(normal_dist_TOXEN()) + 'n\n' +
            
            'x3 out2 out3 vdd vss inverter ' + 'tplv=' + str(normal_dist_length()) + 'n' + ' tpwv=' + str(normal_dist_widthp()) + 'n' + ' tnln=' + str(normal_dist_length()) + 'n' +
            ' tnwm=' + str(normal_dist_length()) + 'n' + ' tpotv=' + str(normal_dist_TOXEP()) + 'n' + ' tnotv=' + str(normal_dist_TOXEN()) + 'n\n' +

            'X4 out3 out4 vdd vss inverter ' + 'tplv=' + str(normal_dist_length()) + 'n' + ' tpwv=' + str(normal_dist_widthp()) + 'n' + ' tnln=' + str(normal_dist_length()) + 'n' +
            ' tnwm=' + str(normal_dist_length()) + 'n' + ' tpotv=' + str(normal_dist_TOXEP()) + 'n' + ' tnotv=' + str(normal_dist_TOXEN()) + 'n\n' +

            'X5 out4 out5 vdd vss inverter ' + 'tplv=' + str(normal_dist_length()) + 'n' + ' tpwv=' + str(normal_dist_widthp()) + 'n' + ' tnln=' + str(normal_dist_length()) + 'n' +
            ' tnwm=' + str(normal_dist_length()) + 'n' + ' tpotv=' + str(normal_dist_TOXEP()) + 'n' + ' tnotv=' + str(normal_dist_TOXEN()) + 'n\n' +

            'X6 out5 out6 vdd vss inverter ' + 'tplv=' + str(normal_dist_length()) + 'n' + ' tpwv=' + str(normal_dist_widthp()) + 'n' + ' tnln=' + str(normal_dist_length()) + 'n' +
            ' tnwm=' + str(normal_dist_length()) + 'n' + ' tpotv=' + str(normal_dist_TOXEP()) + 'n' + ' tnotv=' + str(normal_dist_TOXEN()) + 'n\n' +

            'X7 out6 out7 vdd vss inverter ' + 'tplv=' + str(normal_dist_length()) + 'n' + ' tpwv=' + str(normal_dist_widthp()) + 'n' + ' tnln=' + str(normal_dist_length()) + 'n' +
            ' tnwm=' + str(normal_dist_length()) + 'n' + ' tpotv=' + str(normal_dist_TOXEP()) + 'n' + ' tnotv=' + str(normal_dist_TOXEN()) + 'n\n' +

            'X8 out7 out8 vdd vss inverter ' + 'tplv=' + str(normal_dist_length()) + 'n' + ' tpwv=' + str(normal_dist_widthp()) + 'n' + ' tnln=' + str(normal_dist_length()) + 'n' +
            ' tnwm=' + str(normal_dist_length()) + 'n' + ' tpotv=' + str(normal_dist_TOXEP()) + 'n' + ' tnotv=' + str(normal_dist_TOXEN()) + 'n\n' +

            'X9 out8 out9 vdd vss inverter ' + 'tplv=' + str(normal_dist_length()) + 'n' + ' tpwv=' + str(normal_dist_widthp()) + 'n' + ' tnln=' + str(normal_dist_length()) + 'n' +
            ' tnwm=' + str(normal_dist_length()) + 'n' + ' tpotv=' + str(normal_dist_TOXEP()) + 'n' + ' tnotv=' + str(normal_dist_TOXEN()) + 'n\n' +

            'X10 out9 out10 vdd vss inverter ' + 'tplv=' + str(normal_dist_length()) + 'n' + ' tpwv=' + str(normal_dist_widthp()) + 'n' + ' tnln=' + str(normal_dist_length()) + 'n' +
            ' tnwm=' + str(normal_dist_length()) + 'n' + ' tpotv=' + str(normal_dist_TOXEP()) + 'n' + ' tnotv=' + str(normal_dist_TOXEN()) + 'n\n' +

            'X11 out10 out11 vdd vss inverter ' + 'tplv=' + str(normal_dist_length()) + 'n' + ' tpwv=' + str(normal_dist_widthp()) + 'n' + ' tnln=' + str(normal_dist_length()) + 'n' +
            ' tnwm=' + str(normal_dist_length()) + 'n' + ' tpotv=' + str(normal_dist_TOXEP()) + 'n' + ' tnotv=' + str(normal_dist_TOXEN()) + 'n\n' +

            'X12 out11 vb vdd vss inverter ' + 'tplv=' + str(normal_dist_length()) + 'n' + ' tpwv=' + str(normal_dist_widthp()) + 'n' + ' tnln=' + str(normal_dist_length()) + 'n' +
            ' tnwm=' + str(normal_dist_length()) + 'n' + ' tpotv=' + str(normal_dist_TOXEP()) + 'n' + ' tnotv=' + str(normal_dist_TOXEN()) + 'n\n\n' +

            '.ends ring_oscillator_' + str(i)

            )
        test.close()