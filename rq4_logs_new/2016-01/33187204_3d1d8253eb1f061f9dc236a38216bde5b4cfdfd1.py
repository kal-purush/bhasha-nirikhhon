#!/usr/bin/env python

import sys
Track = []

def buildTrack(line):
    global Track
    for itm in line.split():
        try:
            Track.append(int(itm))
        except ValueError:
            Track.append(float(itm))

def race(top_spd,accel_tm,brake_tm):
    ''' run the race for these specs '''
    global Track
    track,elapsed,dist=list(Track),0.0,0.0
    top = top_spd/3600.0 # seconds, not hours
    accel = top/accel_tm
    brake = top/brake_tm

    v_i=0.0
    while track: # physics
        dist=track.pop(0)
        angle=float(track.pop(0))
        v_f = top*(1-(angle/180.0))

        t_accel = (top - v_i)/accel
        d_accel = v_i*t_accel+0.5*accel*pow(t_accel,2)

        t_decel = (top - v_f)/brake
        d_decel = top*t_decel-0.5*brake*pow(t_decel,2)
        
        d_cruise = dist-d_accel-d_decel
        t_cruise = d_cruise/top
        
        elapsed += t_accel+t_cruise+t_decel
        v_i = v_f

    return elapsed

def main(filename):
    lines=[]
    with open(filename) as f_in: # get only nonempty lines
        lines = [line.strip() for line in f_in if line.rstrip()]

    buildTrack(lines.pop(0))

    results={}
    for line in lines:
        numbr,top_spd,accel_tm,brake_tm=map(
            lambda x,y:x(y),[int,float,float,float],line.split())
        results[numbr]=race(top_spd,accel_tm,brake_tm)

    for itm in sorted(results,key=results.get):
        print "%d %.2f" %(itm,results[itm])

if __name__ == "__main__":
    main(sys.argv[1])