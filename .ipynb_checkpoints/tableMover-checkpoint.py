from Models import player_model, team_model, box_score_model, season_model, shot_chart_model, to_id
from CRUD_Basketball_Reference import db
import basketball_reference_helpers as helpers
import sqlite3
import datetime
from sqlalchemy import func

"""
Copy tables from /home/nathan/Documents/SportsStatsPython/Interfaces/UI_Flask_v1/NBA_Flask_App_old.db
to replace data in /home/nathan/Documents/SportsStatsPython/Interfaces/basketball_reference/NBA_Flask_App.db
"""
conn = sqlite3.connect("/home/nathan/Documents/SportsStatsPython/Interfaces/UI_Flask_v1/NBA_Flask_App_old.db")
cursor = conn.cursor()


def move_schedule():
    schedules = cursor.execute(
        "SELECT date, home, visitor, home_pts, visitor_pts FROM schedule ORDER BY date ASC;").fetchall()
    for index, game in enumerate(schedules):
        date = [int(i) for i in game[0].split(" ")[0].split("-")]  # Drop time and convert date to list of ints, [Y,M,D]
        exists = db.session.query(season_model.schedule).filter_by(
            date=datetime.date(year=date[0], month=date[1], day=date[2]), home_id=helpers.get_team(game[1]).id,
            visitor_id=helpers.get_team(game[2]).id).first() is not None
        if exists:
            continue
        else:
            db.session.add(season_model.schedule(date=datetime.date(year=date[0], month=date[1], day=date[2]),
                                                 season_id=helpers.get_season_id_from_date(date[0], date[1]),
                                                 home_id=helpers.get_team(game[1]).id,
                                                 visitor_id=helpers.get_team(game[2]).id,
                                                 home_pts=game[3],
                                                 visitor_pts=game[4])
                           )
        print(f"Progress, schedule: {str(round(index / len(schedules) * 100, 2))}%")
    db.session.commit()


def move_roster():
    player_list = cursor.execute("SELECT * FROM roster;").fetchall()
    for index, player in enumerate(player_list):
        player_current = helpers.get_player(
            player[10])  # returns player object from db and adds name/id to player table
        player_current.birth_date = player[4].split(' ')[0]  # adds other parts of player object
        player_current.college = player[5]
        player_current.height = player[7]
        player_current.nationality = player[8]
        if type(player[9]) is int:  # handle player_list changing number during season
            number = player[9]
        else:
            try:
                if ',' in player[9]:
                    number = int(player[9].split(',')[
                                     1])  # if a player wore more than one number, save the number they ended the season wearing
                else:
                    number = int(player[9])
            except TypeError:
                number = None  # there are a couple numbers stored in bytes which is an error somewhere

        db.session.add(team_model.roster(team_id=player[2],  # adds roster object
                                         season_id=player[3],
                                         number=number,
                                         player_id=helpers.get_player(player[10]).id,
                                         experience=player[11],
                                         position_id=to_id.position_converter(player[12]),
                                         weight=player[13]
                                         ))
        print(f"Progress, roster: {str(round(index / len(player_list) * 100, 2))}%")
    db.session.commit()


def move_player_per_x():
    player_list = []
    for f in ['_per_game', '_per_minute', '_per_poss']:
        for career in ['', '_career']:
            for playoffs in ['', '_playoffs']:
                new_players = cursor.execute(
                    f"SELECT * FROM player{f}{playoffs}{career} JOIN player_name_to_id ON player{f}{playoffs}{career}.player_id = player_name_to_id.player_id;").fetchall()
                format = helpers.get_format(f[1:]).id
                career_stats = 0 if career == '' else 1  # 0 is not playoffs 1 is
                playoffs = 0 if playoffs == '' else 1
                player_list = player_list + [player + tuple([format, career_stats, playoffs]) for player in new_players]

    for index, p in enumerate(player_list):
        exists = db.session.query(player_model.player_per_x).filter_by(player_id=helpers.get_player(p[25]).id,
                                                                       season_id=p[18],
                                                                       format_id=format).first() is not None

        if exists:
            continue
        else:
            team = helpers.get_team(p[19])
            if team is None:
                db.session.add(team_model.team_info(abbreviation=p[19]))
                db.session.commit()
                team = db.session.query(to_id.team_info).filter_by(abbreviation=p[19]).first()

            season_id = p[17][:2] + p[17][-2:]
            if season_id[3:] == "00":
                season_id = str(int(season_id[:2]) + 1) + "00"

            if season_id.isnumeric():
                pass
            else:
                continue
            db.session.add(player_model.player_per_x(player_id=helpers.get_player(p[25]).id,
                                                     # create new player_per_x object and add it to the db
                                                     team_id=team.id if team is not None else None,
                                                     # player_list don't have a team id when they changed teams during a season
                                                     format_id=p[-3],
                                                     playoffs=p[-1],
                                                     career=p[-2],
                                                     age=p[1],
                                                     ast=p[2],
                                                     blk=p[3],
                                                     drb=p[4],
                                                     fg=p[5],
                                                     fga=p[6],
                                                     ft=p[7],
                                                     fta=p[8],
                                                     g=p[9],
                                                     gs=p[10],
                                                     league=p[11],
                                                     mp=helpers.mp_handler(p[12]),
                                                     orb=p[13],
                                                     pf=p[14],
                                                     pts=p[16],
                                                     season_id=int(p[17][:2] + p[17][-2:]) if (
                                                                 p[17][:2] + p[17][-2:]).isnumeric() else None,
                                                     stl=p[18],
                                                     tov=p[21],
                                                     trb=p[22],
                                                     ))
        print(f"Progress, player_per_x: {str(round(index / len(player_list) * 100, 2))}%")
    db.session.commit()  # commit all


def move_roster_per_x():
    player_list = []
    for f in ['totals', 'per_game', 'per_minute', 'per_poss']:  # formats
        for p in ['_playoffs', '']:  # playoff option
            new_players = cursor.execute(
                f"SELECT ast, blk, drb, fg, fga, ft, fta, g, gs, mp, orb, pf, player, position, pts, season, stl, team, tov, trb FROM roster_{f}{p};").fetchall()
            new_players = [list(i) + [helpers.get_format(f).id] for i in new_players]
            player_list += new_players
    for index, player in enumerate(player_list):
        player_id = helpers.get_player(player[12]).id
        exists = db.session.query(player_model.roster_per_x).filter_by(player_id=player_id, season_id=player[18],
                                                                       format_id=player[-1]).first() is not None

        if exists:
            continue
        else:
            db.session.add(player_model.roster_per_x(ast=player[0],
                                                     # roster_per_x is adding an object called .format and trying to pass it to db. can't figure out where it's coming from or how to get rid of it
                                                     blk=player[1],
                                                     drb=player[2],
                                                     fg=player[3],
                                                     fga=player[4],
                                                     ft=player[5],
                                                     fta=player[6],
                                                     g=player[7],
                                                     gs=player[8],
                                                     mp=helpers.mp_handler(player[9]),
                                                     orb=player[10],
                                                     pf=player[11],
                                                     player_id=player_id,
                                                     pts=player[14],
                                                     season_id=db.session.query(to_id.season_to_id.id).filter_by,
                                                     stl=player[16],
                                                     team_id=helpers.get_team(player[17]).id,
                                                     tov=player[18],
                                                     trb=player[19],
                                                     format_id=player[-1],
                                                     playoffs=False if p == '' else True
                                                     ))
        print(f"Progress, roster_per_x: {str(round(index / len(player_list) * 100, 2))}%")
    db.session.commit()  # commit after all changes are made


def move_player_advanced():
    player_list = []
    for c in ['', '_career']:
        for p in ['', '_playoffs']:
            player_list += cursor.execute(f"SELECT * FROM player_advanced{c}{p}").fetchall()

    for index, p in enumerate(player_list):
        exists = db.session.query(player_model.player_advanced).filter_by(player_id=p[1],
                                                                          season_id=p[4]).first() is not None
        if exists:
            continue
        else:
            try:
                player = helpers.get_player(p[1]).id
                db.session.add(player_model.player_advanced(player_id=player.id,
                                                            career=False if c == '' else True,
                                                            playoffs=False if p == '' else True,
                                                            season_id=player.drafted + p[2] - 1,
                                                            PER=p[3],
                                                            TS_pct=p[4],
                                                            PAr_3=p[5],
                                                            FTr=p[6],
                                                            ORB_pct=p[7],
                                                            DRB_pct=p[8],
                                                            TRB_pct=p[9],
                                                            AST_pct=p[10],
                                                            STL_pct=p[11],
                                                            BLK_pct=p[12],
                                                            USG_pct=p[13],
                                                            OWS=p[14],
                                                            DWS=p[15],
                                                            WS=p[16],
                                                            WS_per_48=p[17],
                                                            OBPM=p[18],
                                                            DBPM=p[19],
                                                            BPM=p[20],
                                                            VORP=p[21]
                                                            ))
            except Exception:
                continue
        print(f"Progress, player_advanced: {str(round(index / len(player_list) * 100, 2))}%")
    db.session.commit()


def move_box_scores():
    formats = ['_game', '_half_1', '_half_2', '_quarter_1', '_quarter_2', '_quarter_3', '_quarter_4', "_advanced"]
    for f in formats:
        new_box_scores = cursor.execute(f"SELECT * FROM box_score{f};").fetchall()
        box_scores = [list(i) + [helpers.get_format(f[1:])] for i in new_box_scores]

        for index, box in enumerate(box_scores):
            if box[-1].format == 'ADVANCED':  # box score is advanced stats
                player_id = helpers.get_player(box[11]).id
                if box[9] == 'Did Not Play':
                    continue
                game_id = helpers.get_game(date=box[5], team1=box[1], team2=box[3]).game_id
                exists = db.session.query(box_score_model.box_score_advanced).filter_by(player_id=player_id,
                                                                                        game_id=game_id).first() is not None
                if exists:
                    continue
                else:
                    with db.session.no_autoflush:
                        db.session.add(box_score_model.box_score_advanced(game_id=game_id,
                                                                          player_id=player_id,
                                                                          team_id=helpers.get_team(box[1]).id,
                                                                          format_id=box[-1].id,
                                                                          bpm=box[6],
                                                                          drtg=box[7],
                                                                          ftr=box[8],
                                                                          mp=helpers.mp_handler(box[9]),
                                                                          ortg=box[10]
                                                                          ))
            else:
                if 'Did Not Play' in box or 'Did Not Dress' in box or 'Not With Team' in box or 'Player Suspended' in box:
                    continue
                player_id = helpers.get_player(box[6]).id
                game_id = helpers.get_game(date=box[1], team1=box[2], team2=box[4]).game_id
                exists = db.session.query(box_score_model.box_score_per_x).filter_by(player_id=player_id,
                                                                                     game_id=game_id,
                                                                                     format_id=box[-1].id).first() is not None
                if exists:
                    continue
                else:
                    with db.session.no_autoflush:
                        db.session.add(box_score_model.box_score_per_x(game_id=game_id,
                                                                       player_id=player_id,
                                                                       team_id=helpers.get_team(box[2]).id,
                                                                       format_id=box[-1].id,
                                                                       ast=box[8],
                                                                       blk=box[9],
                                                                       fg=box[11],
                                                                       fga=box[12],
                                                                       fg_3p=box[14],
                                                                       fg_3pa=box[15],
                                                                       ft=box[17],
                                                                       fta=box[18],
                                                                       mp=helpers.mp_handler(box[20]),
                                                                       orb=box[21],
                                                                       drb=box[10],
                                                                       trb=box[26],
                                                                       pf=box[22],
                                                                       pts=box[23],
                                                                       stl=box[24],
                                                                       tov=box[25],
                                                                       plus_minus=box[27]
                                                                       ))
            print(f"Progress, box_scores, {f[1:]}: {str(round(index / len(box_scores) * 100, 2))}%")
        db.session.commit()


def move_shot_chart():
    teams = db.session.query(to_id.team_info).filter(
        to_id.team_info.id not in db.session.query(shot_chart_model.shot_chart.team_id).distinct()).all()
    for team in teams:
        shots = cursor.execute(
            f"SELECT player_name, date, home, visitor, quarter, distance, make_miss, time_remaining, value, x, y, roster.team FROM shot_chart JOIN team_info on shot_chart.team_id=team_info.team_id JOIN schedule ON shot_chart.game_id=schedule.game_id JOIN roster ON shot_chart.shooter_id=roster.player_id WHERE roster.team='{team.abbreviation}';").fetchall()
        for index, s in enumerate(shots):
            player_id = helpers.get_player(s[0]).id
            game_id = helpers.get_game(s[1], s[2], s[3]).game_id
            time = helpers.mp_handler(s[7])
            exists = db.session.query(shot_chart_model.shot_chart).filter_by(shooter_id=player_id,
                                                                             game_id=game_id,
                                                                             time_remaining=time).first() is not None

            if exists:
                continue
            else:
                db.session.add(shot_chart_model.shot_chart(shooter_id=player_id,
                                                           team_id=team.id,
                                                           game_id=game_id,
                                                           season_id=helpers.get_season_id_from_date(s[1]),
                                                           quarter=s[4],
                                                           distance=int(s[5].split(' ')[0]),
                                                           make=True if s[6] == "MAKE" else False,
                                                           time_remaining=time,
                                                           value=s[8],
                                                           x=s[9],
                                                           y=s[10]
                                                           ))

            print(f"Progress, shot_chart, {team.full_name}: {str(round(index / len(shots) * 100, 2)).format()}%")

        db.session.commit()  # commit once for each team


def move_player_game_log():
    pass  # do box scores cover all game log data?


def move_opponent_per_x():
    opponent_stats = []
    formats = ['_totals', '_per_game']
    for f in formats:
        new = cursor.execute(f"SELECT * FROM opponent{f}").fetchall()
        opponent_stats += [list(i) + [f[1:]] for i in new] # add format to opponent stats

    for index, o in enumerate(opponent_stats):
        team_id = helpers.get_team(o[1]).id
        season_id = o[-2]
        format_id = helpers.get_format(o[-1]).id
        exists = db.session.query(team_model.opponent_per_x).filter_by(team_id=team_id,
                                                                       season_id=season_id,
                                                                       format_id=format_id).first() is not None

        if exists:
            continue
        else:
            db.session.add(team_model.opponent_per_x(team_id=team_id,
                                                     opp_2p=o[3],
                                                     opp_2pa=o[4],
                                                     opp_3p=o[5],
                                                     opp_3pa=o[6],
                                                     opp_ast=o[7],
                                                     opp_blk=o[8],
                                                     opp_drb=o[9],
                                                     opp_fg=o[10],
                                                     opp_ft=o[11],
                                                     opp_fga=o[12],
                                                     opp_fta=o[13],
                                                     opp_g=o[14],
                                                     opp_mp=o[15],
                                                     opp_orb=o[16],
                                                     opp_pf=o[17],
                                                     opp_pts=o[18],
                                                     opp_stl=o[19],
                                                     opp_tov=o[20],
                                                     opp_trb=o[21],
                                                     format_id=format_id,
                                                     season_id=season_id
                                                     ))
        print(f"Progress, opponent_per_x: {str(round(index / len(opponent_stats) * 100, 2)).format()}%")
    db.session.commit()


# TODO finish shot chart tables then move on to game_logs
# fill drafted column in player_list and division column in standings

if __name__ == "__main__":
    move_player_advanced()
    move_shot_chart()
    # move_opponent_per_x()

    # Query for draft year: SELECT p.id, MIN(r.season_id) FROM roster AS r JOIN player_list AS p ON r.player_id=p.id GROUP BY(p.id);
    #player_list = db.session.query(func.min(team_model.roster.season_id), team_model.roster.player_id).group_by(team_model.roster.player_id).all()
    #for player in player_list:
    #   p = helpers.get_player(player[1])
    #    p.drafted = player[0]
    #db.session.commit()
    # move_shot_chart()
    #pass