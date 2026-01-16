import bpy
import csv
import os
from bpy import context
import builtins as __builtin__

def console_print(*args, **kwargs):
    for a in context.screen.areas:
        if a.type == 'CONSOLE':
            c = {}
            c['area'] = a
            c['space_data'] = a.spaces.active
            c['region'] = a.regions[-1]
            c['window'] = context.window
            c['screen'] = context.screen
            s = " ".join([str(arg) for arg in args])
            for line in s.split("\n"):
                bpy.ops.console.scrollback_append(c, text=line)
                
                
def print(*args, **kwargs):
    """Console print() function."""

    console_print(*args, **kwargs) # to py consoles
    __builtin__.print(*args, **kwargs) # to system console

#bpy.context.scene.frame_set(0)

models = bpy.data.objects
scn = bpy.context.scene

positions = [(1.0, 0.0), (4.0, 0.0), (7.0, 0.0), (10.0, 0.0), (13.0, 0.0), (16.0, 0.0), (19, 0.0), (22, 0.0), (25, 0.0), (28, 0.0), (31.0, 0.0)]

l1992 = ["OB", "Samsung", "MBC", "Haitai", "Lotte", "Sami"]

'''
for i,v in enumerate(l1992):
    for model in models:
        if v in model.name:
            model.location[0] = positions[i][0]
'''

        
def season(year, ffrom, incremental, post=True):
    bpy.context.scene.frame_set(ffrom)
    models["Year"].data.text_counter_props.ifAnimated=True
    models["Year"].data.text_counter_props.counter = int(year[:4])
    models["Year"].data.keyframe_insert(data_path='text_counter_props.counter')
    
    
    
    fp = "/Users/hyungsoobae/KBO/{}.csv".format(year)
    final_rank = []
    season_rank = []
    
    with open(fp, 'r', encoding="utf-8") as csvfile:
        rdr = csv.reader(csvfile)
        for i, v in enumerate(rdr):
            #name, percentage
            final_rank.append((v[1], v[6]))
            
            
    season_rank = sorted(final_rank, key=lambda x: x[1], reverse=True)
    
    

    frame = ffrom
    bpy.context.scene.frame_set(frame)
    for t in season_rank:
        team = models[t[0]]
        team.keyframe_insert(data_path='scale')
        team.keyframe_insert(data_path='location')
        
        name = models['{}_Name'.format(team.name)]
        name.keyframe_insert(data_path='location')
        
        try:
            for i in range(2,8):
                try:
                    models['{}_Name_{}'.format(team.name, i)].keyframe_insert(data_path='location')
                except:
                    continue
        except:
            pass
            
        
        point = models['{}_Point'.format(team.name)]
        point.keyframe_insert(data_path='location')
        point.data.text_counter_props.ifAnimated = True
        point.data.text_counter_props.ifDecimal = True
        point.data.text_counter_props.sufix = "%"
        point.data.text_counter_props.decimals = 1
        point.data.text_counter_props.counter = point.data.text_counter_props.counter
        point.data.keyframe_insert(data_path='text_counter_props.counter')
        
        logo = models['{}_Logo'.format(team.name)]
        logo.keyframe_insert(data_path='location')
        try:
            for i in range(2,8):
                try:
                    models['{}_Logo_{}'.format(team.name, i)].keyframe_insert(data_path='location')
                except:
                    continue
        except:
            pass
        
        
        
    frame += incremental
    bpy.context.scene.frame_set(frame)
    for r,t in enumerate(season_rank):
        team = models[t[0]]
        team.scale[2] = float(t[1]) * 4.5
        team.keyframe_insert(data_path='scale')
    
        team.location[0] = positions[r][0]
        team.keyframe_insert(data_path='location')
        
        
        logo = models['{}_Logo'.format(team.name)]
        logo.location[0] = positions[r][0]
        logo.location[2] = team.scale[2] * 2 + 0.4        
        logo.keyframe_insert(data_path='location')
        try:
            for i in range(2,8):
                try:
                    models['{}_Logo_{}'.format(team.name, i)].location[0] = positions[r][0]
                    models['{}_Logo_{}'.format(team.name, i)].location[2] = team.scale[2] * 2 + 0.4  
                    models['{}_Logo_{}'.format(team.name, i)].keyframe_insert(data_path='location')
                except:
                    continue       
        except:
            pass
        
              
        
        point = models['{}_Point'.format(team.name)]
        point.location[0] = positions[r][0]
        if team.scale[2] > 0.5:
            point.location[2] = logo.location[2] - 1.3
        else:
            point.location[2]        
        point.keyframe_insert(data_path='location')

        point.data.text_counter_props.counter = float(float(t[1]) * 100)
        point.data.keyframe_insert(data_path='text_counter_props.counter')        
          

        name = models['{}_Name'.format(team.name)]
        name.location[0] = positions[r][0]
        name.location[2] = logo.location[2] + 2.2
        name.keyframe_insert(data_path='location')
        
        try:
            for i in range(2,8):
                try:
                    models['{}_Name_{}'.format(team.name,i)].location[0] = positions[r][0]
                    models['{}_Name_{}'.format(team.name,i)].location[2] = logo.location[2] + 2.2            
                    models['{}_Name_{}'.format(team.name,i)].keyframe_insert(data_path='location')
                except:
                    continue        
        except:
            pass

    '''
    if post:
        for (t1, t2) in zip(final_rank, season_rank):
            if t1[0] != t2[0]:
                postseason(final_rank, frame, 50)
                break

    '''
    if post:
        postseason(final_rank, frame, 25)

def postseason(final_rank, ffrom, incremental):
    print ("POST SEASON")
    frame = ffrom + 25
    bpy.context.scene.frame_set(frame)
    for t in final_rank:
        team = models[t[0]]
        team.keyframe_insert(data_path='location')
        
        name = models['{}_Name'.format(team.name)]
        name.keyframe_insert(data_path='location')
        try:
            for i in range(2,8):
                try:
                    models['{}_Name_{}'.format(team.name, i)].keyframe_insert(data_path='location')
                except:
                    continue
        except:
            pass
        
        
        point = models['{}_Point'.format(team.name)]
        point.keyframe_insert(data_path='location') 
        
        logo = models['{}_Logo'.format(team.name)]
        logo.keyframe_insert(data_path='location') 
        try:
            for i in range(2,8):
                try:
                    models['{}_Logo_{}'.format(team.name, i)].keyframe_insert(data_path='location')
                except:
                    continue 
        except:
            pass
    
    
    frame += incremental
    bpy.context.scene.frame_set(frame)
    for r,t in enumerate(final_rank):
        team = models[t[0]]
        #team.scale[2] = float(t[1]) * 5
        team.keyframe_insert(data_path='scale')
    
        team.location[0] = positions[r][0]
        team.keyframe_insert(data_path='location')
        
        name = models['{}_Name'.format(team.name)]
        name.location[0] = positions[r][0]
        name.keyframe_insert(data_path='location')
        try:
            for i in range(2,8):
                try:
                    models['{}_Name_{}'.format(team.name,i)].location[0] = positions[r][0]          
                    models['{}_Name_{}'.format(team.name,i)].keyframe_insert(data_path='location')
                except:
                    continue  
        except:
            pass


        
        point = models['{}_Point'.format(team.name)]
        point.location[0] = positions[r][0]
        point.keyframe_insert(data_path='location')
        
        logo = models['{}_Logo'.format(team.name)]
        logo.location[0] = positions[r][0]
        logo.keyframe_insert(data_path='location')
        try:
            for i in range(2,8):
                try:
                    models['{}_Logo_{}'.format(team.name, i)].location[0] = positions[r][0]
                    models['{}_Logo_{}'.format(team.name, i)].keyframe_insert(data_path='location')
                except:
                    continue 
        except:
            pass


def final_season(year, ffrom, incremental, post=True):    
    fp = "/Users/hyungsoobae/KBO/{}.csv".format(year)
    final_rank = []
    season_rank = []
    
    with open(fp, 'r', encoding="utf-8") as csvfile:
        rdr = csv.reader(csvfile)
        for i, v in enumerate(rdr):
            #name, percentage
            final_rank.append((v[1], v[6]))
            
            
    #season_rank = sorted(final_rank, key=lambda x: x[1], reverse=True)
    
    

    frame = ffrom
    bpy.context.scene.frame_set(frame)
    for t in final_rank:
        team = models[t[0]]
        team.keyframe_insert(data_path='scale')
        team.keyframe_insert(data_path='location')
        
        name = models['{}_Name'.format(team.name)]
        name.keyframe_insert(data_path='location')       
        try:
            for i in range(2,8):
                try:
                    models['{}_Name_{}'.format(team.name, i)].keyframe_insert(data_path='location') 
                except:
                    continue
        except:
            pass


        point = models['{}_Point'.format(team.name)]
        point.keyframe_insert(data_path='location')
        point.data.text_counter_props.ifAnimated = True
        point.data.text_counter_props.ifDecimal = True
        point.data.text_counter_props.sufix = "%"
        point.data.text_counter_props.decimals = 1
        point.data.text_counter_props.counter = point.data.text_counter_props.counter
        point.data.keyframe_insert(data_path='text_counter_props.counter')
        
        logo = models['{}_Logo'.format(team.name)]
        logo.keyframe_insert(data_path='location')
        try:
            for i in range(2,8):
                try:
                    models['{}_Logo_{}'.format(team.name, i)].keyframe_insert(data_path='location')
                except:
                    continue
        except:
            pass
        
        
    frame += incremental
    bpy.context.scene.frame_set(frame)
    for r,t in enumerate(final_rank):
        team = models[t[0]]
        team.scale[2] = float(t[1]) * 4.5
        team.keyframe_insert(data_path='scale')
    
        team.location[0] = positions[r][0]
        team.keyframe_insert(data_path='location')
        
        
        logo = models['{}_Logo'.format(team.name)]
        logo.location[0] = positions[r][0]
        logo.location[2] = team.scale[2] * 2 + 0.4        
        logo.keyframe_insert(data_path='location')
        try:
            for i in range(2,8):
                try:
                    models['{}_Logo_{}'.format(team.name, i)].location[0] = positions[r][0]
                    models['{}_Logo_{}'.format(team.name, i)].location[2] = team.scale[2] * 2 + 0.4  
                    models['{}_Logo_{}'.format(team.name, i)].keyframe_insert(data_path='location')
                except:
                    continue
        except:
            pass
        
        point = models['{}_Point'.format(team.name)]
        point.location[0] = positions[r][0]
        if team.scale[2] > 0.5:
            point.location[2] = logo.location[2] - 1.3
        else:
            point.location[2]        
        point.keyframe_insert(data_path='location')

        point.data.text_counter_props.counter = float(float(t[1]) * 100)
        point.data.keyframe_insert(data_path='text_counter_props.counter')        
          

        name = models['{}_Name'.format(team.name)]
        name.location[0] = positions[r][0]
        name.location[2] = logo.location[2] + 2.2
        name.keyframe_insert(data_path='location')      
        try:
            for i in range(2,8):
                try:
                    models['{}_Name_{}'.format(team.name,i)].location[0] = positions[r][0]
                    models['{}_Name_{}'.format(team.name,i)].location[2] = logo.location[2] + 2.2       
                    models['{}_Name_{}'.format(team.name,i)].keyframe_insert(data_path='location') 
                except:
                    continue
        except:
            pass



def add_team(ffrom, team_name, position, incremental):
    frame = ffrom
    bpy.context.scene.frame_set(frame)
    
    team = models[team_name]
    team.keyframe_insert(data_path='location')
    
    logo = models['{}_Logo'.format(team.name)]
    logo.keyframe_insert(data_path='location')
    
    point = models['{}_Point'.format(team.name)]
    point.keyframe_insert(data_path='location')
    
    name = models['{}_Name'.format(team.name)]
    name.keyframe_insert(data_path='location')

    try:
        for i in range(2,8):
            try:
                models['{}_Name_{}'.format(team.name, i)].keyframe_insert(data_path='location')
            except:
                continue
    except:
        pass    
    
    
    frame += incremental
    bpy.context.scene.frame_set(frame)
    
    team.location[0] = positions[position][0]
    team.keyframe_insert(data_path='location')
    
    logo.location[0] = positions[position][0]
    logo.keyframe_insert(data_path='location')    
    
    point.location[0] = positions[position][0]
    point.keyframe_insert(data_path='location')    
    
    name.location[0] = positions[position][0]
    name.keyframe_insert(data_path='location')
    
    try:
        for i in range(2,8):
            try:
                models['{}_Name_{}'.format(team.name, i)].location[0] = positions[position][0]
                models['{}_Name_{}'.format(team.name, i)].keyframe_insert(data_path='location')
            except:
                continue
    except:
        pass    



def remove_team(ffrom, team_name, incremental):
    frame = ffrom
    bpy.context.scene.frame_set(frame)
    
    team = models[team_name]
    team.keyframe_insert(data_path='location')
    
    logo = models['{}_Logo'.format(team.name)]
    logo.keyframe_insert(data_path='location')
    try:
        for i in range(2,8):
            try:
                models['{}_Logo_{}'.format(team.name, i)].keyframe_insert(data_path='location')
            except:
                continue
    except:
        pass    
    
    
    point = models['{}_Point'.format(team.name)]
    point.keyframe_insert(data_path='location')
    
    name = models['{}_Name'.format(team.name)]
    name.keyframe_insert(data_path='location') 
    try:
        for i in range(2,8):
            try:
                models['{}_Name_{}'.format(team.name,i)].keyframe_insert(data_path='location') 
            except:
                continue
    except:
        pass
    
    frame += incremental
    bpy.context.scene.frame_set(frame)
    
    team.location[0] = 50
    team.keyframe_insert(data_path='location')
    
    logo.location[0] = 50
    logo.keyframe_insert(data_path='location')   
    try:
        for i in range(2,8):
            try:
                models['{}_Logo_{}'.format(team.name, i)].location[0] = 50
                models['{}_Logo_{}'.format(team.name, i)].keyframe_insert(data_path='location')
            except:
                continue
    except:
        pass
    
    point.location[0] = 50
    point.keyframe_insert(data_path='location')    
    
    name.location[0] = 50
    name.keyframe_insert(data_path='location')     
    try:
        for i in range(2,8):
            try:
                models['{}_Name_{}'.format(team.name,i)].location[0] = 50 
                models['{}_Name_{}'.format(team.name,i)].keyframe_insert(data_path='location')  
            except:
                continue
    except:
        pass

    
def move_team(ffrom, team_name, incremental, position):
    frame = ffrom
    bpy.context.scene.frame_set(frame)
    
    team = models[team_name]
    team.keyframe_insert(data_path='location')
    
    logo = models['{}_Logo'.format(team.name)]
    logo.keyframe_insert(data_path='location')
    try:
        for i in range(2,8):
            try:
                models['{}_Logo_{}'.format(team.name, i)].keyframe_insert(data_path='location')
            except:
                continue
    except:
        pass      
    
    
    point = models['{}_Point'.format(team.name)]
    point.keyframe_insert(data_path='location')
    
    name = models['{}_Name'.format(team.name)]
    name.keyframe_insert(data_path='location')
    try:
        for i in range(2,8):
            try:
                models['{}_Name_{}'.format(team.name,i)].keyframe_insert(data_path='location') 
            except:
                continue
    except:
        pass
    
    
    frame += incremental
    bpy.context.scene.frame_set(frame)
    
    team.location[0] = positions[position][0]
    team.keyframe_insert(data_path='location')
    
    logo.location[0] = positions[position][0]
    logo.keyframe_insert(data_path='location') 
    try:
        for i in range(2,8):
            try:
                models['{}_Logo_{}'.format(team.name, i)].location[0] = positions[position][0]
                models['{}_Logo_{}'.format(team.name, i)].keyframe_insert(data_path='location')
            except:
                continue
    except:
        pass
    
    point.location[0] = positions[position][0]
    point.keyframe_insert(data_path='location')    
    
    name.location[0] = positions[position][0]
    name.keyframe_insert(data_path='location')
    try:
        for i in range(2,8):
            try:
                models['{}_Name_{}'.format(team.name,i)].location[0] = positions[position][0]
                models['{}_Name_{}'.format(team.name,i)].keyframe_insert(data_path='location')  
            except:
                continue
    except:
        pass


##Two Divisions 1999 2000
p1 = [(1.0, 0.0), (4.0, 0.0), (7.0, 0.0), (10.0, 0.0)]
p2 = [(17.0, 0.0), (20, 0.0), (23, 0.0), (26, 0.0)]

def sseasons(ffrom, incremental, l1, l2, year):
    bpy.context.scene.frame_set(ffrom)
    models["Year"].data.text_counter_props.ifAnimated=True
    models["Year"].data.text_counter_props.counter = int(year)
    models["Year"].data.keyframe_insert(data_path='text_counter_props.counter')    
    
    
    fp = "/Users/hyungsoobae/KBO/{}.csv".format(l1)
    season_rank = []
    
    
    with open(fp, 'r', encoding="utf-8") as csvfile:
        rdr = csv.reader(csvfile)
        for i, v in enumerate(rdr):
            #name, percentage
            season_rank.append((v[1], v[6]))
            
            
    season_rank = sorted(season_rank, key=lambda x: x[1], reverse=True)

    frame = ffrom
    
    bpy.context.scene.frame_set(frame)
    for t in season_rank:
        team = models[t[0]]
        team.keyframe_insert(data_path='scale')
        team.keyframe_insert(data_path='location')
        
        name = models['{}_Name'.format(team.name)]
        name.keyframe_insert(data_path='location')
        try:
            for i in range(2,8):
                try:
                    models['{}_Name_{}'.format(team.name,i)].keyframe_insert(data_path='location')   
                except:
                    continue
        except:
            pass
        
        
        point = models['{}_Point'.format(team.name)]
        point.keyframe_insert(data_path='location')
        point.data.text_counter_props.ifAnimated = True
        point.data.text_counter_props.ifDecimal = True
        point.data.text_counter_props.sufix = "%"
        point.data.text_counter_props.decimals = 1
        point.data.text_counter_props.counter = point.data.text_counter_props.counter
        point.data.keyframe_insert(data_path='text_counter_props.counter')
        
        logo = models['{}_Logo'.format(team.name)]
        logo.keyframe_insert(data_path='location')
        try:
            for i in range(2,8):
                try:
                    models['{}_Logo_{}'.format(team.name, i)].keyframe_insert(data_path='location')
                except:
                    continue
        except:
            pass     
        
    bpy.context.scene.frame_set(frame+incremental)
    for r,t in enumerate(season_rank):
        team = models[t[0]]
        team.scale[2] = float(t[1]) * 4.5
        team.keyframe_insert(data_path='scale')
    
        team.location[0] = p1[r][0]
        team.keyframe_insert(data_path='location')
        
        
        logo = models['{}_Logo'.format(team.name)]
        logo.location[0] = p1[r][0]
        logo.location[2] = team.scale[2] * 2 + 0.4        
        logo.keyframe_insert(data_path='location')
        try:
            for i in range(2,8):
                try:
                    models['{}_Logo_{}'.format(team.name, i)].location[0] = p1[r][0]
                    models['{}_Logo_{}'.format(team.name, i)].location[2] = team.scale[2] * 2 + 0.4 
                    models['{}_Logo_{}'.format(team.name, i)].keyframe_insert(data_path='location')
                except:
                    continue
        except:
            pass   
        
        point = models['{}_Point'.format(team.name)]
        point.location[0] = p1[r][0]
        if team.scale[2] > 0.5:
            point.location[2] = logo.location[2] - 1.3
        else:
            point.location[2]        
        point.keyframe_insert(data_path='location')

        point.data.text_counter_props.counter = float(float(t[1]) * 100)
        point.data.keyframe_insert(data_path='text_counter_props.counter')        
          

        name = models['{}_Name'.format(team.name)]
        name.location[0] = p1[r][0]
        name.location[2] = logo.location[2] + 2.2
        name.keyframe_insert(data_path='location')       
        try:
            for i in range(2,8):
                try:
                    models['{}_Name_{}'.format(team.name,i)].location[0] = p1[r][0]
                    models['{}_Name_{}'.format(team.name,i)].location[2] = logo.location[2] + 2.2      
                    models['{}_Name_{}'.format(team.name,i)].keyframe_insert(data_path='location')    
                except:
                    continue
        except:
            pass





    fp = "/Users/hyungsoobae/KBO/{}.csv".format(l2)
    season_rank = []
    
    
    with open(fp, 'r', encoding="utf-8") as csvfile:
        rdr = csv.reader(csvfile)
        for i, v in enumerate(rdr):
            #name, percentage
            season_rank.append((v[1], v[6]))
            
            
    season_rank = sorted(season_rank, key=lambda x: x[1], reverse=True)

    frame = ffrom
    
    
    bpy.context.scene.frame_set(frame)
    for t in season_rank:
        team = models[t[0]]
        team.keyframe_insert(data_path='scale')
        team.keyframe_insert(data_path='location')
        
        name = models['{}_Name'.format(team.name)]
        name.keyframe_insert(data_path='location')        
        try:
            for i in range(2,8):
                try:
                    models['{}_Name_{}'.format(team.name,i)].keyframe_insert(data_path='location')    
                except:
                    continue        
        except:
            pass
        
        
        point = models['{}_Point'.format(team.name)]
        point.keyframe_insert(data_path='location')
        point.data.text_counter_props.ifAnimated = True
        point.data.text_counter_props.ifDecimal = True
        point.data.text_counter_props.sufix = "%"
        point.data.text_counter_props.decimals = 1
        point.data.text_counter_props.counter = point.data.text_counter_props.counter
        point.data.keyframe_insert(data_path='text_counter_props.counter')
        
        logo = models['{}_Logo'.format(team.name)]
        logo.keyframe_insert(data_path='location')
        try:
            for i in range(2,8):
                try:
                    models['{}_Logo_{}'.format(team.name, i)].keyframe_insert(data_path='location')
                except:
                    continue
        except:
            pass
        
        
    bpy.context.scene.frame_set(frame+incremental)
    for r,t in enumerate(season_rank):
        team = models[t[0]]
        team.scale[2] = float(t[1]) * 4.5
        team.keyframe_insert(data_path='scale')
    
        team.location[0] = p2[r][0]
        team.keyframe_insert(data_path='location')
        
        
        logo = models['{}_Logo'.format(team.name)]
        logo.location[0] = p2[r][0]
        logo.location[2] = team.scale[2] * 2 + 0.4        
        logo.keyframe_insert(data_path='location')
        try:
            for i in range(2,8):
                try:
                    models['{}_Logo_{}'.format(team.name, i)].location[0] = p2[r][0]
                    models['{}_Logo_{}'.format(team.name, i)].location[2] = team.scale[2] * 2 + 0.4 
                    models['{}_Logo_{}'.format(team.name, i)].keyframe_insert(data_path='location')
                except:
                    continue 
        except:
            pass    
        
        point = models['{}_Point'.format(team.name)]
        point.location[0] = p2[r][0]
        if team.scale[2] > 0.5:
            point.location[2] = logo.location[2] - 1.3
        else:
            point.location[2]        
        point.keyframe_insert(data_path='location')

        point.data.text_counter_props.counter = float(float(t[1]) * 100)
        point.data.keyframe_insert(data_path='text_counter_props.counter')        
          

        name = models['{}_Name'.format(team.name)]
        name.location[0] = p2[r][0]
        name.location[2] = logo.location[2] + 2.2
        name.keyframe_insert(data_path='location')
        try:
            for i in range(2,8):
                try:
                    models['{}_Name_{}'.format(team.name,i)].location[0] = p2[r][0]
                    models['{}_Name_{}'.format(team.name,i)].location[2] = logo.location[2] + 2.2    
                    models['{}_Name_{}'.format(team.name,i)].keyframe_insert(data_path='location') 
                except:
                    continue
        except:
            pass



    ## Final Ranking
    fp = "/Users/hyungsoobae/KBO/{}.csv".format(year)
    final_rank = []
    season_rank = []
    
    with open(fp, 'r', encoding="utf-8") as csvfile:
        rdr = csv.reader(csvfile)
        for i, v in enumerate(rdr):
            #name, percentage
            final_rank.append((v[1], v[6]))
            
            
    season_rank = sorted(final_rank, key=lambda x: x[1], reverse=True)
    
    

    frame = ffrom
    
    bpy.context.scene.frame_set(frame+incremental+25)
    for t in final_rank:
        team = models[t[0]]
        team.keyframe_insert(data_path='scale')
        team.keyframe_insert(data_path='location')
        
        name = models['{}_Name'.format(team.name)]
        name.keyframe_insert(data_path='location')
        try:
            for i in range(2,8):
                try:
                    models['{}_Name_{}'.format(team.name,i)].keyframe_insert(data_path='location')
                except:
                    continue
        except:
            pass        
        
        
        point = models['{}_Point'.format(team.name)]
        point.keyframe_insert(data_path='location')
        point.data.text_counter_props.ifAnimated = True
        point.data.text_counter_props.ifDecimal = True
        point.data.text_counter_props.sufix = "%"
        point.data.text_counter_props.decimals = 1
        point.data.text_counter_props.counter = point.data.text_counter_props.counter
        point.data.keyframe_insert(data_path='text_counter_props.counter')
        
        logo = models['{}_Logo'.format(team.name)]
        logo.keyframe_insert(data_path='location')
        try:
            for i in range(2,8):
                try:
                    models['{}_Logo_{}'.format(team.name, i)].keyframe_insert(data_path='location')
                except:
                    continue
        except:
            pass
        
        
    bpy.context.scene.frame_set(frame + incremental + 50 + 15)
    for r,t in enumerate(final_rank):
        team = models[t[0]]
        team.scale[2] = float(t[1]) * 4.5
        team.keyframe_insert(data_path='scale')
    
        team.location[0] = positions[r][0]
        team.keyframe_insert(data_path='location')
        
        
        logo = models['{}_Logo'.format(team.name)]
        logo.location[0] = positions[r][0]
        logo.location[2] = team.scale[2] * 2 + 0.4        
        logo.keyframe_insert(data_path='location')
        try:
            for i in range(2,8):
                try:
                    models['{}_Logo_{}'.format(team.name, i)].location[0] = positions[r][0]
                    models['{}_Logo_{}'.format(team.name, i)].location[2] = team.scale[2] * 2 + 0.4   
                    models['{}_Logo_{}'.format(team.name, i)].keyframe_insert(data_path='location')
                except:
                    continue
        except:
            pass  
        
        point = models['{}_Point'.format(team.name)]
        point.location[0] = positions[r][0]
        if team.scale[2] > 0.5:
            point.location[2] = logo.location[2] - 1.3
        else:
            point.location[2]        
        point.keyframe_insert(data_path='location')

        point.data.text_counter_props.counter = float(float(t[1]) * 100)
        point.data.keyframe_insert(data_path='text_counter_props.counter')        
          

        name = models['{}_Name'.format(team.name)]
        name.location[0] = positions[r][0]
        name.location[2] = logo.location[2] + 2.2
        name.keyframe_insert(data_path='location')      
        try:
            for i in range(2,8):
                try:
                    models['{}_Name_{}'.format(team.name,i)].location[0] = positions[r][0]
                    models['{}_Name_{}'.format(team.name,i)].location[2] = logo.location[2] + 2.2   
                    models['{}_Name_{}'.format(team.name,i)].keyframe_insert(data_path='location') 
                except:
                    continue
        except:
            pass


bpy.context.scene.frame_set(0)


for model in models:
    model.animation_data_clear()
    model.data.animation_data_clear()
    if "Point" in model.name:
        model.data.animation_data_clear()



for model in models:
    if "_Name" in model.name:
        model.data.bevel_resolution=25
        model.data.extrude=0.02
        model.data.bevel_depth = 0.01
        model.data.size = 0.6 
        model.active_material.transparency_method = 'Z_TRANSPARENCY'
        model.active_material.specular_alpha = 0
        pass
    if "_Logo" in model.name:
        model.location[1] = 0



season('1982a',0,50, post=False)
season('1982b',75,50, post=False)
final_season('1982',150,25)

season('1983a',215,50, post=False)
season('1983b',290,50, post=False)
final_season('1983',365,25)

season('1984a',425,50, post=False)
season('1984b',500,50, post=False)
final_season('1984',575,25)

season('1985a',635,50, post=False)
season('1985b',710,50, post=False)
final_season('1985',785,25)


add_team(845, 'Hanhwa', 6, 10)
season('1986a',855,50, post=False)
season('1986b',930,50, post=False)
final_season('1986',1005,25)

season('1987a',1065,50, post=False)
season('1987b',1140,50, post=False)
final_season('1987',1215,25)

season('1988a',1275,50, post=False)
season('1988b',1325,50, post=False)
final_season('1988',1400,25)

season('1989',1460,50)

season('1990',1595,50)

add_team(1730, 'Ssangbangul', 7, 10)
season('1991',1740,50)

season('1992',1875,50)

season('1993',2010,50)

season('1994',2145,50)

season('1995',2280,50)

season('1996',2415,50)

season('1997',2550,50)

season('1998',2685,50)

sseasons(2820, 50, "1999a", "1999b", 1999)

add_team(2970, 'SK', 7, 10)
remove_team(2970, 'Ssangbangul', 10)

sseasons(2980, 50, "2000a", "2000b", 2000)

season('2001',3130,50)


season('2002',3265,50)

season('2003',3400,50)

season('2004', 3535,50)

season('2005', 3670, 50)

season('2006',3805,50)

season('2007',3940,50)

add_team(4075, 'Kiwoom', 7, 10)
remove_team(4075, 'Hyundai', 10)
move_team(4075, 'Lotte', 10, 5)
move_team(4075, 'Kia', 10, 6)

season('2008',4085,50)


season('2009',4220,50)

season('2010', 4355,50)

season('2011',4490,50)

season('2012',4625,50)

add_team(4760, 'NC', 8, 10)
season('2013', 4770, 50)

season('2014', 4905, 50)

add_team(5040, 'KT', 9, 10)

season('2015', 5050, 50)

season('2016', 5185, 50)

season('2017', 5320, 50)

season('2018', 5455, 50)