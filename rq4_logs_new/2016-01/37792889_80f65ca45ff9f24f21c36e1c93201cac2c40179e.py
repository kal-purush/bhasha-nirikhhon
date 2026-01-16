from google.appengine.datastore import datastore_query

from app.api.base import BaseAuthResource
from app.participant.models import ParticipantModel
from flask_restful import fields, marshal, reqparse

LEADERBOARD_TEMPLATE = {
    'name': fields.String,
    'rating': fields.Float,
    'games_played': fields.Integer,
    'wins': fields.Integer,
    'losses': fields.Integer,
    'ties': fields.Integer,
    'k_factor': fields.Integer,
}

SORT_ASCENDING = 'ascending'
SORT_DESCENDING = 'descending'
SORT_OPTIONS = ['name', 'rating', 'games_played', 'k_factor', 'wins', 'losses', 'ties']


class LeaderboardAPI(BaseAuthResource):
    OPTIONAL_ARGS = ['sort_direction', 'sort_by', 'page_size', 'cursor']

    def get(self, league_id):
        """ Return a sorted leaderboard representing participants in a league """
        parser = reqparse.RequestParser()
        parser.add_argument('sort_direction')
        parser.add_argument('sort_by')
        parser.add_argument('page_size', type=int)
        parser.add_argument('cursor', type=datastore_query.Cursor)
        args = parser.parse_args()

        sort_by = args['sort_by']
        sort_direction = args['sort_direction']
        cursor = args['cursor']
        page_size = args['page_size']

        if sort_by and hasattr(ParticipantModel, sort_by) and sort_by in SORT_OPTIONS:
            sort_by = getattr(ParticipantModel, sort_by)
        else:
            sort_by = getattr(ParticipantModel, 'rating')

        if sort_direction == SORT_DESCENDING or not sort_direction:
            leaderboard = ParticipantModel.query(getattr(ParticipantModel, 'league_id') == league_id).order(-sort_by)
        elif sort_direction == SORT_ASCENDING:
            leaderboard = ParticipantModel.query(getattr(ParticipantModel, 'league_id') == league_id).order(sort_by)
        else:
            return 'Invalid sort direction %s' % sort_direction, 400

        results, cursor, more = leaderboard.fetch_page(page_size=int(page_size), start_cursor=cursor)

        return [marshal(l, LEADERBOARD_TEMPLATE) for l in results]