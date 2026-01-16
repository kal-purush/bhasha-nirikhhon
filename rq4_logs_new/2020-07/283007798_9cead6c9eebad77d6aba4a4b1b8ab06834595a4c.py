def undummy ( df , prefix , new_col_name , val_type = float ):

    dummy_cols = [ col for col in df . columns
        if col . startswith ( prefix )]

    idx2val = { i : val_type ( col [ len ( prefix ):]) for i , col
    in enumerate ( dummy_cols )}

    def get_index ( vals ):
        return list ( vals ). index (1)

    ser = df [ dummy_cols ]. apply (
        lambda x : idx2val . get ( get_index ( x ) , None ) , axis =1)
    df [ new_col_name ] = ser
    df = df . drop ( dummy_cols , axis =1)
    return df


undummy ( df , 'Soil_Type' , 'Soil_type')
undummy ( df , 'Wilderness_Area' , 'Wilderness_Area')