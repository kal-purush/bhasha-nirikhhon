bodyweight = {
    'url': 'body/weight',
    'schema': {
        'weight': {
            'type': 'float',
        },
        'datetime': {
            'type': 'datetime',
        },
    }
}

bodyfat = {
    'url': 'body/fat',
    'schema': {
        'fatpct': {
            'type': 'float',
        },
        'datetime': {
            'type': 'datetime',
        },
    }
}

liftingset = {
    'url': 'lifting/set',
    'schema': {
        'exercise': {
            'type': 'string',
        },
        'datetime': {
            'type': 'datetime',
        },
        'weight': {
            'type': 'float',
        },
        'units': {
            'type': 'list',
            'allowed': ["lb","kg"],
        },
        'repetitions': {
            'type': 'int',
        },
    }
}

DOMAIN = {
    'bodyweight': bodyweight,
    'bodyfat': bodyfat,
    'liftingset': liftingset,
}