import os
from typing import Final
from fuzzywuzzy import process
from dotenv import load_dotenv


load_dotenv()

MATCH_ACCEPTANCE_RATIO: Final = int(os.getenv('GENRE_MATCH_ACCEPTANCE_RATIO'))
GENRES: Final = ['ACTION', 'ADVENTURE', 'ANIMATION', 'BIOGRAPHY', 'COMEDY', 'CRIME', 'DOCUMENTARY', 'DRAMA', 'FAMILY', 'FANTASY', 'GAME-SHOW', 'HISTORY',
                 'HORROR', 'MUSIC', 'MUSICAL', 'MYSTERY', 'NEWS', 'REALITY-TV', 'ROMANCE', 'SCI-FI', 'SHORT', 'SPORT', 'TALK-SHOW', 'THRILLER', 'WAR', 'WESTERN']


def genre_match(genres):
    matched_genres = []
    for genre in genres:
        match, ratio = process.extractOne(genre, GENRES)
        matched_genres.append(match) if ratio >= MATCH_ACCEPTANCE_RATIO else None
    return set(matched_genres)