from tokenize import group
from otree.api import *


doc = """
    p_beauty_contest
"""


class C(BaseConstants):
    NAME_IN_URL = 'p_beauty_contest'
    PLAYERS_PER_GROUP = None
    NUM_ROUNDS = 2

    timeout_sec = 30  # 每一回合的決策時間
    timer_sec = 20  # 出現timer的剩餘時間
    alert_sec = 10  # 出現提醒字樣的剩餘時間

    p = 2/3
    min_number = 0
    max_number = 100

    winning_prize = 100
    consolation_prize = 10



class Subsession(BaseSubsession):
    pass


class Group(BaseGroup):
    p_mean_num = models.IntegerField(initial=0)  # 每回合的唯一最小正整數
    num_list = models.StringField(initial="被選到的號碼有：")


class Player(BasePlayer):
    
    guess_num = models.IntegerField(min=C.min_number, max=C.max_number, label='請輸入您所猜測的非負整數：')
    is_winner = models.BooleanField(initial=False)
    
    decision_duration = models.FloatField(initial=0)  # 決策時間
    is_no_decision = models.BooleanField(initial=False)  # 是否有進行決策



# FUNCTIONS
def set_payoffs(group: Group):
    players_guess_dict = {}  # {guess_num: players}
    total = 0
    p_mean = 0
    # 將所有受試者的數字以 dictionary 形式存下來
    for player in group.get_players():
        players_guess_dict[player] = player.guess_num
        total += player.guess_num
        if player != group.get_players()[-1]:
            group.num_list += str(player.guess_num) + "、"
        else:
            group.num_list += str(player.guess_num)

    mean = total / len(group.get_players())
    p_mean = mean * C.p
    min_distance = 100

    for p in group.get_players():
        if abs(players_guess_dict[p] - p_mean) <= min_distance:
            min_distance = abs(players_guess_dict[p] - p_mean)

    
    n_winners = 0
    # 判斷獲勝的受試者，並給予對應的報酬
    for player, num in players_guess_dict.items():

        if (player.is_no_decision == False) and (abs(num - p_mean) == min_distance):
            player.is_winner = True
            n_winners += 1
            group.p_mean_num = num
        else:
            player.payoff = C.consolation_prize

    for player in group.get_players():
        if player.is_winner:
            player.payoff = C.winning_prize / n_winners


# PAGES
class Instruction(Page):
    @staticmethod
    def is_displayed(player):  # built-in methods
        return player.round_number == 1  # 只有 round 1 要有實驗說明

class DecisionPage(Page):
    form_model = 'player'
    form_fields = ['guess_num', 'decision_duration']
    timeout_seconds = C.timeout_sec  # built-in

    @staticmethod
    def before_next_page(player, timeout_happened):  # built-in methods
        if timeout_happened:
            player.is_no_decision = True  # 若回合時間到，將 player 設定為沒有做決策


class ResultsWaitPage(WaitPage): # built-in
    after_all_players_arrive = set_payoffs  # built-in methods，所有受試者都離開決策頁後，執行 set_payoffs


class Results(Page):
    pass


class Finish(Page):
    @staticmethod
    def is_displayed(player):
        return player.round_number == C.NUM_ROUNDS
        
    @staticmethod
    def vars_for_template(player: Player):  # built-in methods，將 total_payoff 的值傳到 html 頁面
        return {
            "total_payoff": sum([p.payoff for p in player.in_all_rounds()])
	    }


page_sequence = [Instruction, DecisionPage, ResultsWaitPage, Results, Finish]
