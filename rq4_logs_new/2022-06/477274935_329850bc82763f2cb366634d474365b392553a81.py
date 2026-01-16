import Scrapper.scrapper as scrapper


# %%
def games_2_csv(first_year: int, last_year: int,location:str):
    months = [ 'January', 'February', 'March', 'April', 'May', 'June', 'July',
     'August', 'September', 'October', 'November', 'December'
    ]
    file = "weekday,month_day,year,hour,Home,HomePts,Visitor,VisitorPts\n"
    for year in range(first_year, last_year):
        print(year)
        for month in months:
            games = scrapper.get_games(year, month)
            if games is None:
                continue
            for game in games:
                if len(game) < 6:
                    continue
                game_list = list()
                game_list.extend(game)
                file += ",".join(game_list) + "\n"
        with open(location, 'w') as csvfile:
            csvfile.write(file)



def team_to_csv(first_year: int, last_year: int,location:str):
    # file = "index,G,MP,FG,FGA,FGP,TP,TPA,tPp,wP,wPA,wPp,FT,FTA,FTp,ORB,DRB,TRB,AST,STL,BLK,TOV,PF\
    #     ,PTS,W,L,PW,PL,MOV,SOS,SRS,ORtg,DRtg,Pace,FTr,3PAr,eFG%,TOV%,ORB%,FT/FGA,eFG%,TOV%,DRB%,FT/FGA,rating\n"
    file = ""
    for year in range(first_year, last_year):
        print(year)
        teams_name, rating = scrapper.get_rating(year)
        for team, letters in teams_name.items():
            print(team)
            row = [str(year) + team]
            team_data = scrapper.get_team_data(
                letters, str(year))
            if team_data is None:
                continue
            row.extend(team_data)
            team_data = scrapper.get_team_misc(
                letters, str(year))
            if team_data is None:
                continue
            row.extend(team_data)
            if team_data is None:
                continue
            row.append(str(rating[team]))
            file += ",".join(row) + "\n"
        with open(location, 'a') as csvfile:
            csvfile.write(file)


def date_to_number(date: str) -> str:
    mounth_to_num ={'Jan': '1', 'Feb': '2', 'Mar': '3', 'Apr': '4', 'May': '5', 'Jun': '6',
     'Jul': '7', 'Aug': '8', 'Sep': '9', 'Oct': '10', 'Nov': '11', 'Dec': '12'}
    date = date.split(',')
    date[1] = date[1].split(' ')
    return mounth_to_num[date[1][1]].zfill(2) + date[1][2].zfill(2) + date[2].strip()


def string_to_int(string: str) -> str:
    return "".join([str(ord(letter)) for letter in string if letter.isalpha() or letter.isdigit()])
