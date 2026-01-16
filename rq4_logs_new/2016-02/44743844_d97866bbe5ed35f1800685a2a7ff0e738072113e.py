# Common attributes settings used by most outing activities
DEFAULT_FIELDS = [
    'locales.title',
    'locales.summary',
    'locales.description',
    'locales.participants',
    'locales.access_comment',
    'locales.weather',
    'locales.timing',
    'locales.conditions_levels',
    'locales.conditions',
    'locales.hut_comment',
    'locales.route_description',
    'geometry.geom',
    'geometry.geom_detail',
    'activities',
    'date_start',
    'date_end',
    'frequentation',
    'participant_count',
    'elevation_min',
    'elevation_max',
    'elevation_access',
    'height_diff_up',
    'height_diff_down',
    'length_total',
    'partial_trip',
    'public_transport',
    'access_condition',
    'lift_status',
    'awesomeness',
    'duration',
    'condition_rating',
    'hut_status'
]
DEFAULT_REQUIRED = [
    'locales',
    'locales.title',
    'activities',
    'date_start',
    'date_end'
]
DEFAULT_LISTING = [
    'locales.title',
    'locales.title_prefix',
    'locales.summary'
]
DEFAULT_ATTRIBUTES_SETTINGS = {
    'fields': DEFAULT_FIELDS,
    'required': DEFAULT_REQUIRED,
    'listing': DEFAULT_LISTING
}

fields_outing = {
    'skitouring': {
        'fields': DEFAULT_FIELDS + [
            'elevation_up_snow',
            'snow_quantity',
            'snow_quality',
            'glacier_rating',
            'avalanche_signs',
            'locales.avalanches',
        ],
        'required': DEFAULT_REQUIRED,
        'listing': DEFAULT_LISTING
    },
    'snow_ice_mixed': {
        'fields': DEFAULT_FIELDS + [
            'elevation_up_snow',
            'elevation_down_snow',
            'duration_difficulties',
            'snow_quantity',
            'snow_quality',
            'glacier_rating',
            'avalanche_signs',
            'locales.avalanches',
        ],
        'required': DEFAULT_REQUIRED,
        'listing': DEFAULT_LISTING
    },
    'mountain_climbing': {
        'fields': DEFAULT_FIELDS + [
            'elevation_up_snow',
            'elevation_down_snow',
            'duration_difficulties',
            'snow_quantity',
            'snow_quality',
            'glacier_rating',
        ],
        'required': DEFAULT_REQUIRED,
        'listing': DEFAULT_LISTING
    },
    'rock_climbing': {
        'fields': DEFAULT_FIELDS + [
            'duration_difficulties',
        ],
        'required': DEFAULT_REQUIRED,
        'listing': DEFAULT_LISTING
    },
    'ice_climbing': {
        'fields': DEFAULT_FIELDS + [
            'elevation_up_snow',
            'elevation_down_snow',
            'duration_difficulties',
            'snow_quantity',
            'snow_quality',
            'avalanche_signs',
            'locales.avalanches',
        ],
        'required': DEFAULT_REQUIRED,
        'listing': DEFAULT_LISTING
    },
    'hiking': {
        'fields': DEFAULT_FIELDS + [
        ],
        'required': DEFAULT_REQUIRED,
        'listing': DEFAULT_LISTING
    },
    'snowshoeing': {
        'fields': DEFAULT_FIELDS + [
            'elevation_up_snow',
            'elevation_down_snow',
            'snow_quantity',
            'snow_quality',
            'glacier_rating',
            'avalanche_signs',
            'locales.avalanches',
        ],
        'required': DEFAULT_REQUIRED,
        'listing': DEFAULT_LISTING
    },
    'moutain_biking': {
        'fields': DEFAULT_FIELDS + [
        ],
        'required': DEFAULT_REQUIRED,
        'listing': DEFAULT_LISTING
    },
    'via_ferrata': {
        'fields': DEFAULT_FIELDS + [
            'duration_difficulties',
            'glacier_rating',
        ],
        'required': DEFAULT_REQUIRED,
        'listing': DEFAULT_LISTING
    }
}