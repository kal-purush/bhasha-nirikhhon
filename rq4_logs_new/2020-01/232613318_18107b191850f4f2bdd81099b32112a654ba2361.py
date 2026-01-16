# -*- coding: utf-8 -*-
"""
IForce force feedback effect parser

This script parses an USB data dump of communication between
a PC and a force feedback joystick speaking IForce (host to
device communication only) and extracts the force effect 
definitions and start/stop commands.

Dump can be captured with e.g. USBPcap and Wireshark.

Text output formats every command with relative timestamp first and
then the command parameters.

SDL output prints out SDL struct initialization and commands with time
stamps in comments.


Copyright (C) 2020 Lauri Peltonen

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.
"""

import codecs
import argparse

def signed(x):
    if x > 127:
        return x - 256
    return x

def signed_short(x):
    if x > 32767:
        return x - 65536
    return x


parser = argparse.ArgumentParser(description='Parse force feedback effect \
                                 descriptions from USB IForce dump to human readable text.')
parser.add_argument('file', help='Filename of the dump (txt)')
parser.add_argument('--sdl', help='Output SDL structs instead', action="store_true")
args = parser.parse_args()
filename = args.file
sdl_out = args.sdl


# Read all the packets from the dump
packets = []
with open(filename, "rt") as fileHandle:
    for line in fileHandle.readlines():
        
        rel_time, hex_string = line.split("\t")
        hex_string = hex_string.strip()

        try:
            rel_time = float(rel_time)
        except:
            # Not a timestamp, ignore
            continue
        
        # Parse the hex string to array of decimals
        try:
            dec_array = []
            for c in codecs.decode(hex_string, "hex"):
                dec_array.append(c)
        except:
            # Something fishy, ignore
            continue
        
        # Store
        packets.append((rel_time, dec_array))


# Parse the packets, according to
# https://www.kernel.org/doc/html/latest/input/devices/iforce-protocol.html
sdl_params = {}     # Store SDL parameter structs
for packet in packets:
    timestamp = packet[0]
    data = packet[1]
    
    if(data[0] == 0x01):      # Force effect
        if (len(data) < 0x0E + 1):
#            print('Erroneous packet length')
#            print(packet)
#            print(timestamp, data)

            continue
        
        channel = data[1]
        effecttype = data[2]        
        trigger = data[3] & 0b00001111
        axesbyte = (data[3] & 0b11110000) >> 3        
        duration = (data[5] << 8) + data[4]
        direction = data[6]
        timebetween = (data[8] << 8) + data[7]
        address1 = (data[10] << 8) + data[9]    # TODO: Or other order?
        address2 = (data[12] << 8) + data[11]   # TODO: Or other order?
        delay = (data[13] << 8) + data[14]
        
        if not sdl_out:
            if(effecttype == 0x00):
                waveform = "Constant Force 0x00"
            elif(effecttype == 0x20):
                waveform = "Square Wave 0x20"
            elif(effecttype == 0x21):
                waveform = "Triangle Wave 0x21"
            elif(effecttype == 0x22):
                waveform = "Sine Wave 0x22"
            elif(effecttype == 0x23):
                waveform = "Sawtooth Up 0x23"
            elif(effecttype == 0x24):
                waveform = "Sawtooth down 0x24"
            elif(effecttype == 0x40):
                waveform = "Spring 40"
            elif(effecttype == 0x41):
                waveform = "Friction And Inertia 0x41"
            else:
                waveform = "Unknown XX"

            if(axesbyte == 0x02):
                axes = "Directional 0x02"
            elif(axesbyte == 0x04):
                axes = "X 0x04"
            elif(axesbyte == 0x08):
                axes = "Y 0x08"
            elif(axesbyte == 0x0C):
                axes = "X and Y 0x0C"
            else:
                axes = "Unknown XX"

            print('{:.2f} Force_effect'.format(timestamp))
            print('\tChannel: {}'.format(channel))
            print('\tWaveform: {}'.format(waveform))
            print('\tTrigger: {}'.format(trigger))
            print('\tAxes: {}'.format(axes))
            print('\tDuration: {} ms'.format(duration))
            print('\tDirection: {} degrees'.format(direction))
            print('\tTime between triggers: {} ms'.format(timebetween))
            print('\tParameter address 1: {}{}'.format(address1, ' (not used)' if address1 == 0xFFFF else ''))
            print('\tParameter address 2: {}{}'.format(address2, ' (not used)' if address2 == 0xFFFF else ''))
            print('\tDelay: {} ms'.format(delay))
        else:
            if(effecttype == 0x00):
                effect_sdl_name = 'SDL_HAPTIC_CONSTANT;'
                effect_class = 'constant'
            elif(effecttype == 0x20):
                effect_sdl_name = 'SDL_HAPTIC_SQUARE; /* if supported by SDL */'
                effect_class = 'periodic'
            elif(effecttype == 0x21):
                effect_sdl_name = 'SDL_HAPTIC_TRIANGLE;'
                effect_class = 'periodic'
            elif(effecttype == 0x22):
                effect_sdl_name = 'SDL_HAPTIC_SINE;'
                effect_class = 'periodic'
            elif(effecttype == 0x23):
                effect_sdl_name = 'SDL_HAPTIC_SAWTOOTHUP;'
                effect_class = 'periodic'
            elif(effecttype == 0x24):
                effect_sdl_name = 'SDL_HAPTIC_SAWTOOTHDOWN;'
                effect_class = 'periodic'
            elif(effecttype == 0x40):
                effect_sdl_name = 'SDL_HAPTIC_SPRING;'
                effect_class = 'condition'
            elif(effecttype == 0x41):
                effect_sdl_name = 'SDL_HAPTIC_FRICTION; /* or SDL_HAPTIC_INERTIA */'
                effect_class = 'condition'
            else:
                print('/* Unknown effect type */')
                continue

            # Common
            print('/* Time: {:.02f} */'.format(timestamp))
            print('SDL_memset(&effect{}, 0, sizeof(SDL_HapticEffect));'.format(channel));
            print('effect{}.type = {}'.format(channel, effect_sdl_name))
            print('effect{}.{}.length = {};'.format(channel, effect_class, duration))
            print('effect{}.{}.delay = {};'.format(channel, effect_class, delay))
            print('effect{}.{}.button = {};'.format(channel, effect_class, trigger))
            print('effect{}.{}.interval = {};'.format(channel, effect_class, timebetween))
            
            # TODO: Should maybe take axis into account, but so far the direction has
            # always been more accurate, even if axis = X, direction may be 225 degrees or so
            print('effect{}.{}.direction.type = SDL_HAPTIC_POLAR;'.format(channel, effect_class))
            print('effect{}.{}.direction.dir[0] = {};'.format(channel, effect_class, int(direction * 100)))

            # Type dependent
            if address1 != 0xffff and address1 in sdl_params:
                for item in sdl_params[address1]:
                    print('effect{}.{}{}'.format(channel, effect_class, item))

            # Attack and delay            
            if effect_class != 'condition' and address2 != 0xffff and address2 in sdl_params:
                for item in sdl_params[address2]:
                    print('effect{}.{}{}'.format(channel, effect_class, item))

            print('effect_id{} = SDL_HapticNewEffect(haptic, &effect{})'.format(channel, channel))


    elif (data[0] == 0x02):     # Attack & fade
        if (len(data) < 0x08 + 1):
            continue
        
        address = (data[2] << 8) + data[1]
        attack_duration = (data[4] <<8) + data[3]
        attack_level = signed(data[5])
        fade_duration = (data[7] << 8) + data[6]
        fade_level = signed(data[8])
        
        if not sdl_out:
            print('{:.2f} Parameter: Attack and fade'.format(timestamp))
            print('\tAddress: {}'.format(address))
            print('\tAttack duration: {} ms'.format(attack_duration))
            print('\tLevel at start of attack: {} ({:.1f} %)'.format(attack_level, attack_level / 1.27))
            print('\tFade duration: {} ms'.format(fade_duration))
            print('\tLevel after fade: {} ({:.1f} %)'.format(fade_level, fade_level / 1.27))
        else:
            sdl_params[address] = ['.attack_length = {};'.format(attack_duration),
                      '.attack_level = {};'.format(int(32767. * attack_level / 127.)),
                      '.fade_length = {};'.format(fade_duration),
                      '.fade_level = {};'.format(int(32767. * fade_level / 127.))]


    elif (data[0] == 0x03):     # Magnitude
        if (len(data) < 0x03 + 1):
            continue
        
        address = (data[2] << 8) + data[1]
        level = signed(data[3])
        
        if not sdl_out:
            print('{:.2f} Parameter: Magnitude'.format(timestamp))
            print('\tAddress: {}'.format(address))
            print('\tLevel: {} ({:.1f} %)'.format(level, level / 1.27))
        else:
            sdl_params[address] = ['.level = {};'.format(int(32767. * level / 127.))]


    elif (data[0] == 0x04):     # Period
        if (len(data) < 0x07 + 1):
            continue
        
        address = (data[2] << 8) + data[1]
        level = signed(data[3])
        offset = signed(data[4])
        phase = data[5]
        period = (data[7] << 8) + data[6]
        
        if not sdl_out:
            print('{:.2f} Parameter: Period'.format(timestamp))
            print('\tAddress: {}'.format(address))
            print('\tMagnitude: {} ({:.1f} %)'.format(level, level / 1.27))
            print('\tOffset: {} ({:.1f} %)'.format(offset, offset / 1.27))
            print('\tPhase: {} ({} deg)'.format(phase, 90.*phase/64.))
            print('\tPeriod: {} ms'.format(period))
        else:
            sdl_params[address] = ['.period = {};'.format(period),
                      '.magnitude = {};'.format(int(32767. * level / 127.)),
                      '.offset = {};'.format(int(32767. * offset / 127)),
                      '.phase = {};'.format(int(9000.0 * phase / 64.0))]
            

    elif (data[0] == 0x05):     # Interactive params
        if (len(data) < 0x0A + 1):
            continue
        
        address = (data[2] << 8) + data[1]
        pos_coeff = signed(data[3])
        neg_coeff = signed(data[4])
        offset = signed_short((data[6] << 8) + data[5])
        deadband = (data[8] << 8) + data[7]
        pos_sat = data[9]
        neg_sat = data[10]
        
        if not sdl_out:
            print('{:.2f} Parameter: Interactive'.format(timestamp))
            print('\tAddress: {}'.format(address))
            print('\tPositive coeff: {} %'.format(pos_coeff))
            print('\tNegative coeff: {} %'.format(neg_coeff))
            print('\tCenter offset: {} ({:.1f} %)'.format(offset, offset / 5.0))
            print('\tDead band: {} % ({})'.format(deadband, deadband*10))
            print('\tPositive saturation: {} ({}, {:.1f} %)'.format(pos_sat, pos_sat*100))
            print('\tNegative saturation: {} ({}, {:.1f} %)'.format(neg_sat, neg_sat*100))
        else:
            sdl_params[address] = ['.right_sat = {};'.format(int(65535. * pos_sat / 255.)),
                      '.left_sat = {};'.format(int(65535. * neg_sat / 255.)),
                      '.right_coeff = {};'.format(int(327.67 * pos_coeff)),
                      '.left_coeff = {};'.format(int(327.67 * neg_coeff)),
                      '.deadband = {};'.format(int(65.535 * deadband)),
                      '.center = {};'.format(int(32767. * offset / 500.))]


    elif (data[0] == 0x41):     # Control
        if (len(data) < 0x03 + 1):
            continue
        
        channel = data[1]
        val = data[2]
        iterations = data[3]
        
        if not sdl_out:
            if (val == 0x00):
                mode = "Stop 0x00"
            elif (val == 0x01):
                mode = "Play Once 0x01"
            elif (val == 0x41):
                mode = "Play N Times 0x41"
            else:
                mode = "Unknown XX"

            print('{:.2f} Control Command'.format(timestamp))
            print('\tChannel: {}'.format(channel))
            print('\tStart/stop: {}'.format(mode))
            print('\tIterations: {}'.format(iterations))
        else:
            print('/* Time: {:.02f} */'.format(timestamp))
            if (val == 0x00):
                print('SDL_HapticStopEffect(haptic, effect_id{});'.format(channel))
            elif (val == 0x01):
                print('SDL_HapticRunEffect(haptic, effect_id{}, 1);'.format(channel))
            elif (val == 0x41):
                print('SDL_HapticRunEffect(haptic, effect_id{}, {});'.format(channel, iterations))
        
        
    elif (data[0] == 0x42):     # Set state
        if (len(data) < 0x01 + 1):
            continue

        if not sdl_out:        
            state = 'Stop all' if data[1] & 0x01 else ''
            state += ' Enable FF' if data[1] & 0x02 else ''
            state += ' Pause FF' if data[1] & 0x04 else ''
            
            print('{:.2f} Set effect state'.format(timestamp))
            print('\tState: {}'.format(state))
        else:
            print('/* Time: {:.02f} */'.format(timestamp))
            if (data[1] & 0x01):
                print('SDL_HapticStopAll(haptic);')
            if (data[1] & 0x02):
                print('SDL_HapticUnpause(haptic);')
            if (data[1] & 0x04):
                print('SDL_HapticPause(haptic);')


    elif (data[0] == 0x43):     # Set gain
        if(len(data) < 0x01 + 1):
            continue
        
        gain = data[1]
        
        if not sdl_out:
            print('{:.2f} Set gain'.format(timestamp))
            print('\tGain: {} ({:.1f} %)'.format(gain, gain/1.28))
        else:
            print('SDL_SetGain(haptic, {});'.format(int(gain/1.28)))