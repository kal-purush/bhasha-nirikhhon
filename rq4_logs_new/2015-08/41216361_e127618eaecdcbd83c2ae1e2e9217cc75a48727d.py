def get_index(string, note):
    notes = ['E','F','F#/Gb','G','G#/Ab','A','A#/Bb', 'B', 'C', 'C#/Db', 'D',  'D#/Eb']
    note_i = notes.index(note)

    open_string = 0

    if string == 6:
        if note_i == 0:
            return 12
        else:
            return note_i
    elif string == 5:
        if note_i == 5:
            return 17 
        open_string = 5
    elif string == 4:
        if note_i == 10:
            return 22
        open_string = 10
    elif string == 3:
        if note_i == 3:
            return 27
        open_string = 15
    elif string == 2:
        if note_i == 7:
            return 19
        open_string = 19
    elif string == 1:
        if note_i == 0:
            return 36 
        open_string = 24 
        
    while note_i < open_string:
        note_i += 12

    return note_i