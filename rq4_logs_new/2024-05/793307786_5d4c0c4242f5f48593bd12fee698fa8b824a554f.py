import time

from sqlalchemy import func

from app.database import get_db
from app.domain.restapi.tables import Summoner, User, Participant, Match


class SummonerRepository:

    def __init__(self):
        self.db = next(get_db())

    async def find_summoner_and_user(self):
        return self.db.query(Summoner, User).join(User, Summoner.puuid == User.puuid).first()

    async def find_summoners_rank_by_puuids(self, user_puuids: list[str]):
        return self.db.query(
            Summoner.puuid,
            Summoner.id,
            Summoner.game_name,
            Summoner.tag_line,
            Summoner.profile_icon,
            Summoner.level,
            Summoner.solo_tier,
            Summoner.solo_lp,
            Summoner.solo_wins,
            Summoner.solo_loses,
            Summoner.flex_tier,
            Summoner.flex_lp,
            Summoner.flex_wins,
            Summoner.flex_loses,
            Summoner.most1,
            Summoner.most2,
            Summoner.most3,
            Summoner.last_updated,
            Summoner.rank_point,
            func.rank().over(
                order_by=Summoner.rank_point.desc(),
                partition_by=Summoner.puuid
            ).label("ranking"),
        ).filter(Summoner.puuid.in_(user_puuids)).all()

    async def find_summoner_rank(self, game_name: str, tag_line: str):
        return (self.db.query(
            Summoner.puuid,
            Summoner.id,
            Summoner.game_name,
            Summoner.tag_line,
            Summoner.profile_icon,
            Summoner.level,
            Summoner.solo_tier,
            Summoner.solo_lp,
            Summoner.solo_wins,
            Summoner.solo_loses,
            Summoner.flex_tier,
            Summoner.flex_lp,
            Summoner.flex_wins,
            Summoner.flex_loses,
            Summoner.most1,
            Summoner.most2,
            Summoner.most3,
            Summoner.last_updated,
            Summoner.rank_point,
            func.rank().over(
                order_by=Summoner.rank_point.desc(),
                partition_by=Summoner.puuid
            ).label("ranking"),
        )
                .filter(Summoner.game_name == game_name)
                .filter(Summoner.tag_line == tag_line)
                .first())

    async def find_lol_chang(self):
        now = int(str(time.time()).split('.')[0])
        summoner = (self.db.query(
            Participant.puuid.label("puuid"),
            Summoner.game_name.label("game_name"),
            Summoner.tag_line.label("tag_line"),
            Summoner.profile_icon.label("profile_icon"),
            Summoner.level.label("level"),
            Summoner.solo_tier.label("solo_tier"),
            Summoner.solo_lp.label("solo_lp"),
            Summoner.solo_wins.label("solo_wins"),
            Summoner.solo_loses.label("solo_loses"),
            Summoner.flex_tier.label("flex_tier"),
            Summoner.flex_lp.label("flex_lp"),
            Summoner.flex_wins.label("flex_wins"),
            Summoner.flex_loses.label("flex_loses"),
            Summoner.most1.label("most1"),
            Summoner.most2.label("most2"),
            Summoner.most3.label("most3"),
            func.count(Summoner.puuid).label("played_games"),
            func.sum(Participant.win).label('win_games'),
            (func.sum(Participant.puuid) - func.sum(Participant.win)).label("lose_games"),
            func.sum(Participant.kill).label("kill"),
            func.sum(Participant.death).label("death"),
            func.sum(Participant.assist).label("assist"),
            func.sum(Participant.spent_gold).label("gold_spent"),
            func.sum(Participant.skill_used).label("skill_use"),
            func.sum(Match.game_duration).label("play_time"),
            (func.sum(Participant.spell1_used) + func.sum(Participant.spell2_used)).label("spell_use"),
            func.sum(Participant.damage).label("damage"),
            func.sum(Participant.gain_damage).label("gain_damage"),
            (func.sum(Participant.sight_ward) + func.sum(Participant.vision_ward)).label("ward_place"),
            func.sum(Participant.vision_score).label("vision_score"),
            func.sum(Participant.cs).label("cs"),
            func.row_number().over(
                order_by=Summoner.rank_point.desc(),
                partition_by=Summoner.puuid
            ).label("ranking")
        )
                    .select_from(Participant)
                    .join(Match)
                    .join(Summoner, Participant.puuid == Summoner.puuid)
                    .join(User, Participant.puuid == User.puuid)
                    .filter(Match.game_type != "아레나")
                    .filter(Match.game_start_at >= now - 604800)
                    .group_by(Participant.puuid)
                    .order_by(func.count(Participant.puuid).desc())
                    .first())
        return summoner