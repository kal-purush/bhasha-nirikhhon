from blackjack import deal_cards, follow_command, draw_table, PlayerWins, DealerWins, Push


def play():

    game = deal_cards()

    while True:

        print draw_table()

        if game["player_sum"] <= 11:
            issue_command("hit")
        elif game["player_sum"] >= 17:
            issue_command("stay")

        else:
            for card in game["dealer_hand"]:
                if card.facing == "up":  # no peeking!
                    if card.rank <= 6:
                        issue_command("stay")
                    else:
                        issue_command("hit")

def issue_command(cmd):
    print "decided to", cmd
    follow_command(cmd)

if __name__ == '__main__':
    play()