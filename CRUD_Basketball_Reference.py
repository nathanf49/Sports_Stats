import datetime
import random
import logging
from basketball_reference_scraper import players, seasons, teams, box_scores, pbp, shot_charts
import requests.exceptions
from Models import season_model, shot_chart_model, player_model, team_model, box_score_model, to_id
import flask_sqlalchemy
from flask import Flask
import basketball_reference_helpers as helpers
import time
from sqlalchemy import desc, func, asc
import os

app = Flask(__name__)
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False  # Terminal warning that this will be deprecated soon
app.config[
    'SQLALCHEMY_DATABASE_URI'] = "postgresql://postgres:postgres@localhost/NBA_Stats_Basketball_Reference"  # dialect://user:password@host/db_name

db = flask_sqlalchemy.SQLAlchemy(app)

log_path = os.getcwd() + '/logs/'
logging.basicConfig(filename= log_path + 'CRUD_log', level=logging.INFO, format='%(levelname)s - %(datetime.datetime.now())s - %(message)s')


def create_schedule(start_year=datetime.date.today().year, end_year=None):
    """
    Write all the scheduled games from year1 through year2 or just year1 if there is only 1 input
    Starts in 1949/50 season
    """
    if end_year is None:
        end_year = start_year + 1

    logging.info(f'create_schedule(start_year={start_year}, end_year={end_year})')

    for season in range(start_year, end_year):
        time.sleep(random.randint(50, 250) / 100)
        year = seasons.get_schedule(season)
        time.sleep(2)
        for game_index in range(len(year)):
            exists = db.session.query(season_model.schedule).filter_by(date=year['DATE'][game_index],
                                                                       home_id=year['HOME'][game_index],
                                                                       visitor_id=year[
                                                                           'VISITOR']).first() is not None  # checks if team is already in table
            if exists:
                pass  # team is already in table. No need to add
            else:
                db.session.add(season_model.schedule(date=year['DATE'][game_index],
                                                     home_id=year['HOME'][game_index],
                                                     visitor_id=year['VISITOR'][game_index],
                                                     home_pts=helpers.int_cast(year['HOME_PTS'][game_index]),
                                                     visitor_pts=helpers.int_cast(year['VISITOR_PTS'][
                                                                                      game_index])))  # add new season to scheduled commits
        db.session.commit()  # commit changes after each season. session doesn't seem to handle multiple seasons
        logging.info(f"Adding schedule for {season}")


def update_schedule():
    """
    Attempts to  create missing games from schedule starting from the maximum season currently present in the table
    """
    max_year = int(str(db.session.query(func.max(season_model.schedule.date)).first()[0]).split(' ')[0][:4])
    # max_date = cursor.execute('SELECT MAX(date) FROM schedule;').fetchone()[0]
    current = datetime.date.today().year + 1  # range function in create_schedule would not include final year
    create_schedule(current, max_year)


def create_team_misc(season_id=datetime.date.today().year):  # created 2022
    """
    Writes miscellaneous data from each season.
    season_end_year can be a str or integer
    """
    team_list = helpers.get_all_teams()

    logging.info(f'create_team_misc({season_id})')

    for team in team_list:
        time.sleep(random.randint(50, 250) / 100)
        data = teams.get_team_misc(team.abbreviation, season_id)
        if data is None:
            continue
        exists = db.session.query(team_model.team_misc).filter_by(season_id=str(data['SEASON']),
                                                                  team_id=team.id).first() is not None
        if exists:
            continue
        else:
            db.session.add(
                team_model.team_misc(arena=data['ARENA'], attendance=data['ATTENDANCE'], ftr=data['FTr'],
                                     w=data['W'], l=data['L'], mov=data['MOV'], nrtg=data['NRtg'], ortg=data['ORtg'],
                                     drtg=data['DRtg'], ts=data['TS%'], par_3=data['PAr'], efg_pct=data['eFG%'][0],
                                     tov_pct=data['TOV%'][0], orb_pct=data['ORB%'], dreb_pct=data['DRB%'],
                                     ft_fga=data['FT/FGA'][0], attendance_per_game=data['ATTENDANCE/G'],
                                     pace=data['PACE'], pl=data['PL'], pw=data['PW'], season_=str(data['SEASON']),
                                     sos=data['SOS'], srs=data['SRS'], team_id=team.id))
    db.session.commit()
    logging.info(f"Committing team_misc for {season_id}")


def create_roster(season_start=None, season_end=helpers.get_season_id_from_date(str(datetime.date.today()))):
    """
    Creates table of team rosters
    season_start and season_end should both be integers.
    Data starts in 1947
    """
    if season_start is None:  # try to update
        season_start = team_model.roster.query.order_by(desc(team_model.roster.season_id)).first().season_id
        if season_start is None:  # if table is not built, start from first year of data
            season_start = 1947

    logging.info(f'create_roster(season_start={season_start}, season_end={season_end})')

    for season in range(season_start, season_end + 1):
        team_list = db.session.query(team_model.team_info).all()
        for t in team_list:
            time.sleep(random.randint(50, 250) / 100)
            data = teams.get_roster(t.abbreviation, season)
            if data is not None:
                time.sleep(1)  # timeout to avoid disconnection
                for player_index in range(len(data['PLAYER'])):
                    if db.session.query(to_id.players).filter_by(name=data["PLAYER"][
                            player_index]).first() is None:  # check if player exists in player_name_to_id and adds if not
                        db.session.add(to_id.players(name=data["PLAYER"][player_index]))

                        logging.info(f"{data['PLAYER'][player_index]} added to roster")
                    player_id = helpers.get_player(data['PLAYER'][player_index]).id  # select player id
                    exists = db.session.query(team_model.roster).filter_by(team_id=t.id,
                                                                           player_id=player_id,
                                                                           season_id=int(season)).first() is not None
                    if exists:
                        continue
                    else:
                        db.session.add(team_model.roster(team_id=t.id,
                                                         birth_date=data['BIRTH_DATE'][player_index],
                                                         college=data['COLLEGE'][player_index],
                                                         experience=data['EXPERIENCE'][player_index],
                                                         height=data['HEIGHT'][player_index],
                                                         nationality=data['NATIONALITY'][player_index],
                                                         number=helpers.int_cast(data['NUMBER'][player_index]),
                                                         player_id=player_id,
                                                         position=data['POS'][player_index],
                                                         weight=helpers.int_cast(data['WEIGHT'][player_index]),
                                                         season_id=str(season)))
            else:
                continue  # occurs for invalid seasons from teams that didn't exist that year and too many requests
        db.session.commit()  # commit season at a time to make it easier to pickup from the right spot if function exits
        logging.info(f"Committing roster for {season}")


def create_standings(starting_date=None):  # created through 1970-10-13
    """
    starting_date - Desired date in a string of YYYY-MM-DD (e.g. '2020-01-06').
    Default value is NONE, which returns all standings not already in standings
    Inputting a date will get all standings from every day a game was played after that
    """

    if starting_date is not None:
        dates = db.session.query(season_model.schedule.date).filter(season_model.schedule.date > starting_date,
                                                                    season_model.schedule.date not in db.session.query(
                                                                        season_model.standings.date)).order_by(
            season_model.schedule.date).distinct().all()
    else:
        starting_date = 'None'
        dates = db.session.query(season_model.schedule.date).filter(
            season_model.schedule.date not in db.session.query(season_model.standings.date)).order_by(
            season_model.schedule.date).distinct().all()

    logging.info(f'create_standings(starting_date={starting_date})')

    dates = [helpers.datetime_to_date_str(i) for i in dates]

    for index, date in enumerate(dates):
        time.sleep(random.randint(50, 250) / 100)
        standings = seasons.get_standings(date)
        if standings is None:
            continue
        current_division = None
        for key in standings.keys():
            for i in range(len(standings[key]['TEAM'])):  # index 0 is always a division
                if 'Division' in standings[key]['GB'][i]:
                    current_division = helpers.get_locale(standings[key]["GB"][i])
                    continue
                team = helpers.get_team(standings[key]['TEAM'][i].replace('*', ''))
                try:
                    exists = db.session.query(season_model.standings).filter_by(team_id=team.id,
                                                                                locale_id=current_division.id if current_division is not None else helpers.get_locale(
                                                                                    key).id,
                                                                                date=date).first() is not None
                    if exists:
                        continue
                    else:
                        db.session.add(season_model.standings(
                            team_id=team.id,
                            gb=float(standings[key]["GB"][i]) if standings[key]["GB"][i] != "—" else 0,
                            locale_id=current_division.id if current_division is not None else helpers.get_locale(
                                key).id,
                            w=helpers.int_cast(standings[key]['W'][i]),
                            l=helpers.int_cast(standings[key]['L'][i]),
                            date=date))
                except ValueError:
                    continue  # some stats seem to be saved incorrectly. For example the 'Orlando Magic' never played 'Southeast Division' with a result of 'Southeast Division'

        db.session.commit()  # commit changes after each set/date of rankings are saved
        logging.info(f"Committing standings for {date}")


def create_shot_chart(game_id=None):  # created shot charts from game 1 through game 29941
    """
    writes all shot charts to NBA_Stats_Flask_APP.shot_chart
    starts at game_id and continues to the past if a game_id is input.
    Game_id can be a string or an int
    """
    if game_id is None:
        games = db.session.query(func.max(season_model.schedule.game_id)).first()
        game_id = 'game_id'
    else:
        games = db.session.query(func.max(season_model.schedule.game_id)).filter(
            season_model.schedule.game_id >= int(game_id)).order_by(season_model.schedule.game_id.asc()).all()

    logging.info(f'create_shot_chart(game_id={game_id})')

    for index, g in enumerate(games):
        time.sleep(random.randint(50, 250) / 100)
        game = helpers.get_game(g[0])
        team1 = helpers.get_team(game.home_id)
        team2 = helpers.get_team(game.visitor_id)
        try:
            data = shot_charts.get_shot_chart(game.date, team1.abbreviation, team2.abbreviation)
        except AttributeError:  # getting none type attribute error in basketball_reference_scraper where there is no data
            continue
        if data is not None:
            for team in data.keys():
                for shot_index in range(len(data[team]['DISTANCE'])):
                    if type(data[team]['PLAYER'][shot_index]) is str:
                        shooter_id = helpers.get_player(data[team]['PLAYER'][shot_index].replace("'", "")).id
                    else:
                        shooter_id = None
                    if data[team]['VALUE'][shot_index] is None:
                        value = None
                    else:  # cast value as a helpers.int_cast(), so it doesn't convert to BLOB data
                        try:
                            value = helpers.int_cast(data[team]['VALUE'][shot_index])
                        except ValueError:
                            continue

                    exists = db.session.query(shot_chart_model.shot_chart).filter_by(
                        distance=data[team]['DISTANCE'][shot_index],
                        make_miss=data[team]['MAKE_MISS'][shot_index],
                        shooter_id=shooter_id,
                        team_id=helpers.get_team(team).id,
                        quarter=helpers.int_cast(data[team]['QUARTER'][shot_index]),
                        time_remaining=data[team]['TIME_REMAINING'][shot_index],
                        value=value,
                        game_id=game.id,
                        x=data[team]['x'][shot_index].split()[0],
                        y=data[team]['y'][shot_index].split()[0]).first() is not None
                    if exists:
                        continue
                    else:
                        db.session.add(shot_chart_model.shot_chart(distance=data[team]['DISTANCE'][shot_index],
                                                                   make_miss=data[team]['MAKE_MISS'][shot_index],
                                                                   shooter_id=shooter_id,
                                                                   team_id=helpers.get_team(team).id,
                                                                   quarter=helpers.int_cast(
                                                                       data[team]['QUARTER'][shot_index]),
                                                                   time_remaining=data[team]['TIME_REMAINING'][
                                                                       shot_index],
                                                                   value=value,
                                                                   game_id=game.id,
                                                                   x=data[team]['x'][shot_index].split()[0],
                                                                   y=data[team]['y'][shot_index].split()[0]))
        db.session.commit()  # commit each game in case of error late in process
        logging.info(f'Committing game: {team1.full_name} vs. {team2.full_name} - {game.date}')


def create_team_per_x(start_season=1951, format="PER_GAME"):
    # PER_POSS Throws Index Error on any input, even those that work for Totals and Per_Game, so skipping that for now
    """
    Creates table for team "PER_GAME", "TOTALS", "PER_MINUTE", "PER_POSS
    Accepts start season as a helpers.int_cast() to go forward from or starts from the oldest season in schedule
    Default is 1951 because there is no data before that
    Input the oldest year in your table and the function will create data moving toward present day
    """

    logging.info(f'create_team_per_x(start_season={start_season}, format={format})')
    season_list = db.session.query(season_model.schedule).distinct(season_model.schedule.season_id).filter(
        season_model.schedule.season_id > start_season)
    team_list = helpers.get_all_teams()
    format = helpers.get_format(format)
    for season in season_list:
        for team in team_list:
            time.sleep(random.randint(50, 250) / 100)

            data = teams.get_team_stats(team.abbreviation, helpers.int_cast(season[0]), format.format)
            exists = db.session.query(team_model.team_per_x).filter_by(season_id=helpers.get_season_id_from_date(
                data['SEASON']),
                team_id=team.id).first() is not None
            if exists:
                continue
            else:
                db.session.add(team_model.team_per_x(ast=data['AST'],
                                                     blk=data['BLK'],
                                                     drb=data['DRB'],
                                                     fg=data['FG'],
                                                     fga=data['FGA'],
                                                     ft=data['FT'],
                                                     fta=data['FTA'],
                                                     g=data['G'],
                                                     mp=helpers.mp_handler(data['MP']),
                                                     orb=data['ORB'],
                                                     pf=data['PF'],
                                                     season_id=helpers.get_season_id_from_date(data['SEASON']),
                                                     stl=data['STL'],
                                                     team_id=team.id,
                                                     tov=data['TOV'],
                                                     trb=data['TRB']))
            logging.info(f"Adding {format} data for {team} in {season}")
    db.session.commit()  # commit all teams from each season at once for per_game and total to keep tables easy to update and even
    logging.info(f'Committing {format} data for all teams starting in {start_season}')


def create_opponent_per_x(update_from=1949, format="TOTALS"):
    """
    Creates table for opponent per_x stats
    Accepts update_ from as a helpers.int_cast() to go forward from or starts from the oldest season in schedule
    Input the oldest year in your table and the function will create data moving toward present day
    Data starts in 1950
    """
    season_list = db.session.query(season_model.schedule).distinct(season_model.schedule.season_id).filter(
        season_model.schedule.season_id > update_from)
    team_list = helpers.get_all_teams()
    logging.info(f"create_opponent_per_x(update_from={update_from}, format={format})")
    for season in season_list:
        for team in team_list:
            time.sleep(random.randint(50, 250) / 100)
            try:
                data_per_game = teams.get_opp_stats(team.abbreviation, helpers.int_cast(season[0]), format)
                exists = db.session.query(team_model.opponent_per_x).filter_by(team_id=team.id,
                                                                               format_id=helpers.get_format(format),
                                                                               season_id=str(
                                                                                   season[0])).first() is not None
                if exists:
                    continue
                else:
                    db.session.add(team_model.opponent_per_x(team_id=team.id,
                                                             opp_2p=data_per_game['OPP_2P'],
                                                             opp_2pa=data_per_game['OPP_2PA'],
                                                             opp_3p=data_per_game['OPP_3P'],
                                                             opp_3pa=data_per_game['OPP_3PA'],
                                                             opp_ast=data_per_game['OPP_AST'],
                                                             opp_blk=data_per_game['OPP_BLK'],
                                                             opp_drb=data_per_game['OPP_DRB'],
                                                             opp_fg=data_per_game['OPP_FG'],
                                                             opp_ft=data_per_game['OPP_FT'],
                                                             opp_fga=data_per_game['OPP_FGA'],
                                                             opp_fta=data_per_game['OPP_FTA'],
                                                             opp_g=data_per_game['OPP_G'],
                                                             opp_mp=data_per_game['OPP_MP'],
                                                             opp_orb=data_per_game['OPP_ORB'],
                                                             opp_pf=data_per_game['OPP_PF'],
                                                             opp_pts=data_per_game['OPP_PTS'],
                                                             opp_stl=data_per_game['OPP_STL'],
                                                             opp_tov=data_per_game['OPP_TOV'],
                                                             opp_trb=data_per_game['OPP_TRB'],
                                                             format_id=helpers.get_format(format),
                                                             season_id=str(season[0])
                                                             ))
            except (ValueError, IndexError):
                helpers.int_cast("PER_GAME " + str(season[0]))

        db.session.commit()  # commit all teams from each season at once for per_game and total to keep tables easy to update and even
        logging.info(f'Committing all opponent {format} data from {season}')


def create_roster_per_x(format_id="PER_GAME", update_from=1949, playoffs=False, team_list=None):
    """
    Creates per_game tables and inserts players into player_name_to_id
    if they are not in that table already. No data before 1949, so that is the minimum starting date
    """
    format = helpers.get_format(format_id)
    if format.format.upper() == "PER_GAME" and update_from < 1949:
        raise Exception("roster per_game stats start in 1949")
    elif format.format.upper() == "PER_MINUTE" and update_from < 1951:
        raise Exception("roster per_minute starts in 1951")
    elif format.format.upper() == "PER_POSS" and update_from < 1972:
        raise Exception("roster per_poss starts in 1972")
    elif format.format.upper() == "TOTALS" and update_from < 1949:
        raise Exception("roster totals start in 1949")
    else:
        pass  # no error with date

    if team_list is not None and type(team_list) is not list:
        team_list = [helpers.get_team(team_list)]
    elif team_list is None:
        team_list = helpers.get_all_teams()

    logging.info(
        f"create_roster_per_x(format_id={format_id}, update_from={update_from}, playoffs={playoffs}, team_list={team_list})")

    season_list = db.session.query(season_model.schedule).distinct(season_model.schedule.season_id).filter_by(
        season_model.schedule.season_id > update_from)
    for season in season_list:
        for team in team_list:
            time.sleep(random.randint(50, 250) / 100)
            season = helpers.int_cast(season.id)
            try:
                roster_per_x = teams.get_roster_stats(team=team.abbreviation,
                                                      season_end_year=season,
                                                      data_format=format.upper(),
                                                      playoffs=playoffs)

                for player_index in range(len(roster_per_x['PLAYER'])):
                    player_id = helpers.get_player(name=roster_per_x['PLAYER']).id
                    exists = db.session.query(player_model.roster_per_x.filter_by(
                        player_id=player_id,
                        position_id=to_id.position_converter(roster_per_x['POS'][player_index]),
                        season_id=str(season),
                        team_id=team.id,
                        format_id=helpers.get_format(format))).first() is not None

                    if exists:
                        continue
                    else:
                        db.session.add(player_model.roster_per_x(age=roster_per_x['AGE'][player_index],
                                                                 ast=roster_per_x['AST'][player_index],
                                                                 blk=roster_per_x['BLK'][player_index],
                                                                 drb=roster_per_x['DRB'][player_index],
                                                                 fg=roster_per_x['FG'][player_index],
                                                                 fga=roster_per_x['FGA'][player_index],
                                                                 ft=roster_per_x['FT'][player_index],
                                                                 fta=roster_per_x['FTA'][player_index],
                                                                 g=roster_per_x['G'][player_index],
                                                                 gs=roster_per_x['GS'][player_index],
                                                                 mp=roster_per_x['MP'][player_index],
                                                                 orb=roster_per_x['ORB'][player_index],
                                                                 pf=roster_per_x['PF'][player_index],
                                                                 player_id=player_id,
                                                                 position=to_id.position_converter(
                                                                     roster_per_x['POS'][player_index]),
                                                                 pts=roster_per_x['PTS'][player_index],
                                                                 season_id=str(season),
                                                                 stl=roster_per_x['STL'][player_index],
                                                                 team_id=team.id,
                                                                 tov=roster_per_x['TOV'][player_index],
                                                                 trb=roster_per_x['TRB'][player_index],
                                                                 format_id=helpers.get_format(format)
                                                                 ))

            except (ValueError, AttributeError) as error:
                print(error)
                print(f'No {format} stats for {team} for {season}')
                continue  # some years only have data but don't have any of the other stat types
        db.session.commit()  # commit season by season
        logging.info(f'Committing roster {format} data for all teams {season}')


def create_box_score_per_x(period="GAME", game_id=None):  # 2017 - 2022 all formats
    """
    Collects all box scores for given period.
    Period can be Q1-4, H1 or H2, or GAME
    If game_id has no input, all games will be checked to see if they exist.
    If game_id has an input, all games from the given id through the present will be checked
    """
    if game_id is None:
        data = db.session.query(season_model.schedule).order_by(season_model.schedule.game_id.asc()).all()
        # data = cursor.execute('SELECT home, visitor, date from schedule ORDER BY game_id ASC').fetchall()

    else:
        data = db.session.query(season_model.schedule).filter(season_model.schedule.game_id > game_id).order_by(
            season_model.schedule.game_id.asc()).all()
        # data = cursor.execute('SELECT home_id, visitor_id, date from schedule WHERE game_id > ' + str(game_id) + ' ORDER BY game_id ASC;').fetchall()

    logging.info(f"create_box_score_per_x(period={period}, game_id={game_id})")

    for game in data:
        time.sleep(random.randint(50, 250) / 100)
        home = helpers.get_team(game.home_id)
        visitor = helpers.get_team(game.visitor_id)
        date = str(game.date).split()[0]
        try:
            box_score = box_scores.get_box_scores(date=date,
                                                  team1=home.abbreviation,
                                                  team2=visitor.abbreviation,
                                                  period=period)
        except ValueError:  # occurs when no tables are found for requested game
            continue
        if box_score is not None:
            for team in box_score.keys():
                for player_index in range(len(box_score[team]['PLAYER'])):
                    if type(box_score[team]['MP'][
                                player_index]) is str:  # players who didn't play have nan for all values which is a float value
                        exists = db.session.query(box_score_model.box_score_per_x).filter_by(
                            game_id=helpers.get_game(date=date,
                                                     team1=team,
                                                     team2=box_score.keys().remove(team)[0]),
                            team_id=helpers.get_team(team).id,
                            player_id=helpers.get_player(box_score[team]['PLAYER'][player_index]).id,
                            date=date,
                            period=period).first() is not None
                        if exists:
                            continue
                        else:
                            db.session.add(box_score_model.box_score_per_x(game_id=helpers.get_game(date=date,
                                                                                                    team1=team,
                                                                                                    team2=box_score.keys().remove(team)[0]),
                                                                           team_id=helpers.get_team(team).id,
                                                                           player_id=helpers.get_player(
                                                                               box_score[team]['PLAYER'][
                                                                                   player_index]).id,
                                                                           ast=box_score[team]['AST'][player_index],
                                                                           blk=box_score[team]['BLK'][player_index],
                                                                           drb=box_score[team]['DRB'][player_index],
                                                                           fg=box_score[team]['FG'][player_index],
                                                                           fga=box_score[team]['FGA'][player_index],
                                                                           ft=box_score[team]['FT'][player_index],
                                                                           fta=box_score[team]['FTA'][player_index],
                                                                           mp=box_score[team]['MP'][player_index],
                                                                           orb=box_score[team]['ORB'][player_index],
                                                                           pf=box_score[team]['PF'][player_index],
                                                                           pts=box_score[team]['PTS'][player_index],
                                                                           stl=box_score[team]['STL'][player_index],
                                                                           tov=box_score[team]['TOV'][player_index],
                                                                           trb=box_score[team]['TRB'][player_index],
                                                                           plus_minus=box_score[team]['+/-'][
                                                                               player_index],
                                                                           period=period
                                                                           ))
        else:
            continue
        db.session.commit()  # commit full games
        logging.info(f'Committing box score for {home} vs {visitor}, {date}, {period}')


def create_box_score_advanced(game_id=None):
    """
    Date should be in YYYY-MM-DD format
    This function starts from the given game_id and moves forward from that game collecting advanced stats for each
    game in season_model.schedule
    """

    if game_id is None:
        games = db.session.query(season_model.schedule).order_by(season_model.schedule.game_id.asc()).all()

    else:
        games = db.session.query(season_model.schedule).filter(season_model.schedule.game_id > game_id).order_by(
            season_model.schedule.game_id.asc()).all()

    logging.info(f"create_box_score_advanced(game_id={game_id})")

    for game in games:
        time.sleep(random.randint(50, 250) / 100)
        try:
            box_score = box_scores.get_box_scores(date=game.date,
                                                  team1=helpers.get_team(game.home_id).abbreviation,
                                                  team2=helpers.get_team(game.visitor_id).abbreviation,
                                                  period='GAME',
                                                  stat_type='ADVANCED')
        except ValueError:  # no table found. Continue
            continue
        if box_score.keys() is not None:
            for team in box_score.keys():
                for player_index in range(len(box_score[team]['PLAYER'])):
                    if box_score[team]['MP'][player_index] == 'Did Not Play' or box_score[team]['MP'][
                            player_index] == 'Did Not Dress':  # skip players who didn't play
                        continue
                    exists = db.session.query(box_score_model.box_score_advanced).filter_by(
                        game_id=game.id,
                        team_id=helpers.get_team(team).id,
                        player_id=helpers.get_player(box_score[team]['PLAYER'][player_index]).id).first() is not None
                    if exists:
                        continue
                    else:
                        db.session.add(box_score_model.box_score_advanced(bpm=box_score[team]['BPM'][player_index],
                                                                          game_id=game.id,
                                                                          team_id=helpers.get_team(team).id,
                                                                          drtg=box_score[team]['DRtg'][player_index],
                                                                          ftr=box_score[team]['FTr'][player_index],
                                                                          mp=box_score[team]['MP'][player_index],
                                                                          ortg=box_score[team]['ORtg'][player_index],
                                                                          player_id=helpers.get_player(
                                                                              box_score[team]['PLAYER'][
                                                                                  player_index]).id))
        else:
            continue
        db.session.commit()  # commit game by game
        logging.info(f'Committed {helpers.get_team(game.home_id).full_name} vs {helpers.get_team(game.visitor_id).full_name}, {game.date}, advanced')


def create_player_game_logs(name=None, playoffs=False):
    """
    Creates player game logs for a single player or all players if there is no name input
    name can be a string or a list of strings
    start_season is not required, but will start at 1971, the beginning of the game log data
    playoff game log data begins in 2000
    if unfilled move toward present day
    """
    if name is None:  # SELECTS players who played in given season
        player_list = db.session.query(team_model.roster.player_id).filter(
            team_model.roster.player_id not in db.session.query(player_model.game_log.player_id),
            player_model.game_log.playoffs == playoffs).distinct().all()

    elif type(name) is str:  # converts type of name if user inputs one name as a string
        player_list = [helpers.get_player(name)]

    else:
        raise TypeError(
            "Name should be a string containing a players name or id or None to get the game logs for all players")

    logging.info(f"create_player_game_logs(name={name}, playoffs={playoffs})")

    for p in player_list:
        time.sleep(random.randint(50, 250) / 100)
        career = helpers.get_career_seasons(
            p.name)  # find out which seasons player was in the league to cut down on wasted queries
        for season in range(career[0], career[1] + 1):
            try:
                game_logs = players.get_game_logs(_name=p.full_name, year=season,
                                                  playoffs=playoffs)  # getting attribute error that NoneType has no attribute replace for everything
                time.sleep(2)  # wait between 1 and 5 seconds to avoid attempts to disconnect
            except (UnboundLocalError, IndexError,
                    AttributeError):  # Unbound error when player did not play in the season or Index Error/Attribute error in package for NoneType
                time.sleep(30 * 60)  # sleep to get results to come in again
                continue
            if game_logs is None or game_logs.empty:  # skip emtpy dataframes
                continue
            for game_index in range(len(game_logs['DATE'])):
                if playoffs is False:  # add to regular season table
                    exists = db.session.query(player_model.game_log).filter_by(
                        game_id=helpers.get_game(date=game_logs['DATE'][game_index],
                                                 team1=game_logs['TEAM'][game_index],
                                                 team2=game_logs['OPPONENT'][game_index]),
                        player_id=p.id).first() is not None
                    if exists:
                        continue
                    else:
                        db.session.add(
                            player_model.game_log(game_id=helpers.get_game(date=game_logs['DATE'][game_index],
                                                                           team1=game_logs['TEAM'][game_index],
                                                                           team2=game_logs['OPPONENT'][game_index]),
                                                  player_id=p.id,
                                                  ast=game_logs['AST'][game_index],
                                                  blk=game_logs.get('BLK', [None] * (game_index + 1))[game_index],
                                                  # not recorded in old games, returns None instead with dict.get()
                                                  drb=game_logs.get('DRB', [None] * (game_index + 1))[game_index],
                                                  # not recorded in old games
                                                  fg=game_logs['FG'][game_index],
                                                  fga=game_logs['FGA'][game_index],
                                                  ft=game_logs['FT'][game_index],
                                                  fta=game_logs['FTA'][game_index],
                                                  game_score=game_logs.get("GAME_SCORE", [None] * (game_index + 1))[
                                                      game_index],  # not recorded in old games
                                                  game_started=game_logs['GS'][game_index],
                                                  minutes_played=game_logs['MP'][game_index],
                                                  orb=game_logs.get('ORB', [None] * (game_index + 1))[game_index],
                                                  # not recorded in old games
                                                  pf=game_logs['PF'][game_index],
                                                  pts=game_logs['PTS'][game_index],
                                                  result=game_logs['RESULT'][game_index],
                                                  playoffs=playoffs,
                                                  stl=game_logs.get('STL', [None] * (game_index + 1))[game_index],
                                                  team_id=helpers.get_team(game_logs['TEAM'][game_index]).id,
                                                  turnovers=game_logs.get('TOV', [None] * (game_index + 1))[game_index],
                                                  # not recorded in old games
                                                  trb=game_logs['TRB'][game_index]
                                                  ))

        db.session.commit()  # commit player by player
        logging.info(f"Committed {p.name})")


def create_player_per_x(format="PER_GAME", playoffs=False, career=False, shuffle=False):
    """
    Can specify a player as a string or player_id.
    Can also specify years with start season or end season as helpers.int_cast()s.
    Function will include start_season and go up to but will not include end season
    playoffs and career are both false by default but can be made TRUE to get just
    playoff games or just a view of the full career instead of each season

    Returns every specified players per game averages for every season they played.
    Collects all players for every season if no inputs are given
    """
    format = helpers.get_format(format)
    player_list = db.session.query(to_id.players).filter(
        to_id.players.id not in db.session.query(player_model.player_per_x).filter(
            player_model.player_per_x.playoffs == playoffs, player_model.player_per_x.career == career,
            player_model.player_per_x.format_id == format.id)).all()
    if shuffle is True: # shuffle players to avoid checking the same players repeatedly
        random.shuffle(player_list)
    failures = 0

    logging.info(f"create_player_per_x(format={format}, playoffs={playoffs}, career={career})")

    for p in player_list:
        time.sleep(random.randint(50, 250) / 100)
        try:
            stats = players.get_stats(p.name, stat_type='PER_GAME', playoffs=playoffs, career=career)
        except IndexError:  # error from package
            continue
        if stats.empty:  # skip empty Dataframes
            continue
        for season_index in range(len(stats['AGE'])):
            if type(stats['FTA'][season_index]) == str:  # skip lines for 'Did Not Play\xa0(other pro league—Spain)':
                continue
            if type(stats['SEASON'][season_index]) is str:  # avoid issue presenting season_id as  '7 seasons'
                stats['SEASON'][season_index] = int(stats['SEASON'][season_index].replace(' seasons', ''))

            exists = db.session.query(player_model.player_per_x).filter_by(player_id=p.id,
                                                                           age=stats.get('AGE')[season_index],
                                                                           mp=stats.get('MP')[season_index],
                                                                           playoffs=playoffs,
                                                                           career=career,
                                                                           format_id=helpers.get_format(format).id
                                                                           ).first() is not None  # check if record exists before inserting it again
            if exists:
                continue
            else:
                try:
                    db.session.add(player_model.player_per_x(age=stats['AGE'][season_index],
                                                             ast=stats.get('AST')[season_index],
                                                             blk=stats.get('BLK')[season_index] if stats[
                                                                                                       'BLK'] is not None else None,
                                                             drb=stats.get('DRB')[season_index] if stats[
                                                                                                       'DRB'] is not None else None,
                                                             fg=stats.get('FG')[season_index],
                                                             fga=stats.get('FGA')[season_index],
                                                             ft=stats.get('FT')[season_index],
                                                             fta=stats.get('FTA')[season_index],
                                                             g=helpers.int_cast(stats.get('G')[season_index]),
                                                             gs=helpers.int_cast(stats.get('GS')[season_index]),
                                                             league=stats.get('LEAGUE')[season_index],
                                                             mp=stats.get('MP')[season_index],
                                                             orb=stats.get('ORB')[season_index] if stats.get(
                                                                 "ORB") is not None else None,
                                                             pf=stats.get('PF')[season_index],
                                                             pts=stats.get('PTS')[season_index],
                                                             season_id=stats.get('SEASON')[season_index],
                                                             stl=stats.get('STL')[season_index] if stats.get(
                                                                 "STL") is not None else None,
                                                             team_id=helpers.get_team(
                                                                 stats.get('TEAM')[season_index]).id,
                                                             tov=stats.get('TOV')[season_index] if stats.get(
                                                                 "TOV") is not None else None,
                                                             trb=stats.get('TRB')[season_index],
                                                             player_id=p.id,
                                                             playoffs=playoffs,
                                                             career=career,
                                                             format_id=format.id
                                                             ))
                except Exception as e:  # keep track of how many failures there are
                    failures += 1
                    Warning(
                        f"Failure {failures} for {p.name} in season {season_index}, format: {format.format}. Stacktrace: {e})")
                    continue
        try:
            db.session.commit()  # commit full player career at a time
            logging.info(f'Committing {p.name} - per_{format}')
        except:
            db.session.flush()
            continue


def create_player_advanced(playoffs=False, career=False):
    """
    Similar to create_player_stats, but creates advanced stats for players instead
    Only gets regular season games by default, gets only playoff games if playoffs is set to True
    Gets advanced data for player's whole career if career is set to True
    """

    player_list = db.session.query(to_id.players.id).filter(
        to_id.players.id not in db.session.query(player_model.player_advanced.player_id)).all()

    logging.info(f"create_player_advanced(playoffs={playoffs}, career={career})")

    for p in player_list:
        time.sleep(random.randint(50, 250) / 100)
        try:
            stats = players.get_stats(p.name, stat_type='ADVANCED', playoffs=playoffs, career=career)
        except IndexError:  # error from package
            continue
        if stats is None or stats.empty:  # skip dataframes with no data
            continue
        for season_index in range(len(stats['SEASON'])):
            # Set below variables to stated values when they exist or to None if they don't exist
            OBPM = helpers.check_column(stats, 'OBPM', season_index)
            DBPM = helpers.check_column(stats, 'DBPM', season_index)
            BPM = helpers.check_column(stats, 'BPM', season_index)
            VORP = helpers.check_column(stats, 'VORP', season_index)
            PAr = helpers.check_column(stats, '3PAr', season_index)

            exists = db.session.query(player_model.player_advanced).filter_by(player_id=p.id,
                                                                              career=career,
                                                                              playoffs=playoffs,
                                                                              season_id=stats['SEASON'][
                                                                                  season_index]).first() is not None  # check if record exists before inserting it again
            if exists:
                continue
            else:
                db.session.add(player_model.player_advanced(player_id=p.id,
                                                            career=career,
                                                            playoffs=playoffs,
                                                            age=stats['AGE'][season_index],
                                                            team=stats['TEAM'][season_index],
                                                            league=stats['LEAGUE'][season_index],
                                                            PER=stats['PER'][season_index],
                                                            TS_Pct=stats['TS%'][season_index],
                                                            Par_3=PAr,
                                                            FTr=stats['FTr'][season_index],
                                                            ORB_pct=stats['ORB%'][season_index],
                                                            DRB_pct=stats['DRB%'][season_index],
                                                            TRB_pct=stats['TRB%'][season_index],
                                                            AST_pct=stats['AST%'][season_index],
                                                            STL_pct=stats['STL%'][season_index],
                                                            BLK_pct=stats['BLK%'][season_index],
                                                            USG_pct=stats['USG%'][season_index],
                                                            OWS=stats['OWS'][season_index],
                                                            DWS=stats['DWS'][season_index],
                                                            WS=stats['WS'][season_index],
                                                            WS_per_48=stats['WS/48'][season_index],
                                                            OBPM=OBPM,
                                                            DBPM=DBPM,
                                                            BPM=BPM,
                                                            VORP=VORP,
                                                            season_id=stats['SEASON'][season_index]
                                                            ))
        db.session.commit()  # commit full player career at a time
        logging.info("Committed player_advanced")


def create_pbp(game_id=None):
    """
    Creates play by player data for all available games if game_id is None or from all games
    from given id through present if a game_id is given
    Starts with game_id 32140 or start of 1996/97 season
    """
    with app.app_context():
        if game_id is None:
            games = db.session.query(season_model.schedule.game_id).filter(
                season_model.schedule.game_id not in db.session.query(team_model.play_by_play).distinct().order_by(
                    asc(season_model.schedule.date))).distinct().all()
            # cursor.execute("SELECT date, home, visitor FROM schedule WHERE game_id NOT IN (SELECT DISTINCT(game_id) FROM pbp) ORDER BY date ASC;")
        else:
            games = db.session.query(season_model.schedule).filter(season_model.schedule.game_id > game_id,
                                                                   season_model.schedule.game_id not in db.session.query(
                                                                       team_model.play_by_play.game_id)).order_by(
                asc(season_model.schedule.date)).distinct().all()
            # cursor.execute(f"SELECT date, home, visitor, game_id FROM schedule WHERE game_id > {game_id} AND game_id NOT IN (SELECT DISTINCT(game_id) FROM pbp ORDER BY date ASC;")
        # games = cursor.fetchall()
        if len(games) == 0:
            raise Warning("No available games to update pbp. Consider updating schedule.")
        # games = [helpers.get_game(i[0]) for i in games]  # create list of game objects instead of just ids

        logging.info(f"create_pbp(game_id={game_id})")

    for game in games:
        time.sleep(random.randint(50, 250) / 100)
        try:
            pbp_stats = pbp.get_pbp(date=game.date.date(),
                                    team1=helpers.get_team(game.home_id).abbreviation,
                                    team2=helpers.get_team(game.visitor_id).abbreviation)

        except AttributeError:  # package throws AttributeError if there is no data, just move to next game
            continue

        for play_index in range(len(pbp_stats['TIME_REMAINING'])):
            play = helpers.play_breakdown(pbp_stats.iloc[[play_index]])
            if type(pbp_stats['QUARTER'][play_index]) is not int:
                q = int(pbp_stats['QUARTER'][play_index].replace("OT", "")) + 4
            else:
                q = pbp_stats['QUARTER'][play_index]

            exists = db.session.query(team_model.play_by_play).filter_by(quarter=q,
                                                                         game_id=game.game_id,
                                                                         time_remaining=pbp_stats['TIME_REMAINING'][
                                                                             play_index],
                                                                         action=play['ACTION'],
                                                                         acting_team_id=helpers.get_team(
                                                                             play['ACTING TEAM']).id,
                                                                         # get ID from full name
                                                                         acting_team_score=play['ACTING TEAM SCORE'],
                                                                         opponent_team_id=helpers.get_team(
                                                                             play['OPPONENT']).id,
                                                                         opponent_score=play['OPPONENT SCORE']
                                                                         ).first() is not None
            if exists:
                continue

            else:
                db.session.add(team_model.play_by_play(quarter=q,
                                                       game_id=game.game_id,
                                                       time_remaining=pbp_stats['TIME_REMAINING'][play_index],
                                                       action=play['ACTION'],
                                                       acting_team_id=play['ACTING TEAM'],
                                                       acting_team_score=play['ACTING TEAM SCORE'],
                                                       opponent_team_id=play['OPPONENT'],
                                                       opponent_score=play['OPPONENT SCORE']))

        db.session.commit()
        logging.info(f'Committed pbp for game_id: {game.game_id}')


def update_box_score_advanced():
    game = db.session.query(func.max(box_score_model.box_score_advanced.date)).first()[0]
    logging.info(f"update_box_score_advanced(), game={game})")
    create_box_score_per_x(game)


def update_box_score_per_x(period, playoffs=False):
    game_id = db.session.query(func.max(box_score_model.box_score_per_x.game_id)).filter(
        box_score_model.box_score_per_x.format_id == helpers.get_format(period),
        box_score_model.box_score_per_x.playoffs == playoffs).first()[0]
    # game_id = cursor.execute(f"SELECT MAX(game_id) FROM box_score_per_x WHERE period = {period} AND playoffs = {playoffs}").fetchone()[0]
    logging.info(f"update_box_score_per_x(period={period}, playoffs={playoffs}, game_id = {game_id})")
    create_box_score_per_x(game_id=game_id, period=period)


def update_pbp():
    """
    Updates pbp from max current game_id
    starts at game_id 32140
    """
    game_id = db.session.query(func.max(team_model.play_by_play.game_id)).first()[0]
    logging.info(f"update_pbp from {game_id})")
    create_pbp(game_id)


def update_all_box_scores():
    """
    period = Q1, Q2, Q3, Q4, H1, H2, GAME
    playoffs = True, False
    """
    logging.info(f'update_all_box_scores')
    for period in ["Q1", "Q2", "Q3", "Q4", "H1", "H2", "GAME"]:
        for playoffs in [True, False]:
            update_box_score_per_x(period, playoffs)
    update_box_score_advanced()


def update_roster_per_x(format="TOTALS", playoffs=False):
    """
    format_id=TOTALS, PER_GAME, PER_MIN, PER_POSS
    playoffs = True, False
    """
    max_season = db.session.query(func.max(player_model.roster_per_x.season_id)).filter(
        player_model.roster_per_x.format_id == helpers.get_format(format).id,
        player_model.roster_per_x.playoffs == playoffs).first()[0]
    logging.info(f"update_roster_per_x(format={format}, playoffs={playoffs}), max_season={max_season})")
    create_roster_per_x(max_season)


def update_opponent_per_x(format):
    """
    Format can be a string from "PER_GAME", "PER_MINUTE", "PER_POSS" or ids for those formats
    """
    format = helpers.get_format(format)
    max_season = db.session.query(func.max(team_model.opponent_per_x.season_id)).filter(
        team_model.opponent_per_x.format_id == format.id).first()[0]
    logging.info(f"update_opponent_per_x(format={format}, max_season={max_season})")
    create_opponent_per_x(max_season, format.format)


def update_shot_chart():
    """
    Checks for new shot_charts
    """
    max_game = db.session.query(func.max(shot_chart_model.shot_chart.game_id)).first()[0]
    logging.info(f"update_shot_chart, max_game={max_game})")
    create_shot_chart(max_game + 1)


def update_roster():
    """
    Updates roster table. Automatically chooses max_season presently in table
    """
    season_start = db.session.query(func.max(team_model.roster.season_id)).first()[0]
    logging.info(f'Updating roster from {season_start}')
    create_roster(season_start=season_start)


def add_player_start_end(player_list=None, new_players_only=True, update_roster_prior=False, shuffle=False):
    """
    If using a player list, players can be ids or strings
    Adds start season and end season to players table
    Relies on roster table to find start and end
    player_list should be a list of player names or ids to update
    new_players_only will skip any players that already have data for last_season_id
    update_roster_prior will update the roster tables before starting
    shuffle will shuffle the order the players are in before starting
    """

    if update_roster_prior:  # option to update roster table that helper.get_career_seasons relies upon
        logging.info("Updating roster before adding player start/end ids")
        update_roster()
        db.session.commit()

    logging.info(f'add_player_start_end(new_players_only={new_players_only}, update_roster_prior={update_roster_prior}, shuffle={shuffle})')
    if player_list is None:  # generate players for user
        if new_players_only:  # gets players that don't already have data
            nba_players = db.session.query(to_id.players).filter(to_id.players.id not in db.session.query(to_id.players.id).filter(to_id.players.last_season_id > 0)).all()

        else:
            nba_players = db.session.query(to_id.players).distinct().all()

    else:  # if user passed their own players, make sure the list is all player objects
        if type(player_list) is not list:
            player_list = [player_list]
        nba_players = [helpers.get_player(i) for i in player_list]

    if shuffle:  # shuffle order players will be added before starting
        random.shuffle(nba_players)

    for p in nba_players:
        if p.name is None:
            continue
        season_ids = helpers.get_career_seasons(p.name)
        p.first_season_id = season_ids[0]
        p.last_season_id = season_ids[1]

        logging.info(f'start/end season_id added for {p.name}')

    db.session.commit()
    logging.info(f'Committed changes for add_player_start_end')

def update_player_end(seasons_back=3, start=None, update=True):
    """
    Gets players who have a last season within a range that they could still be playing
    Relies on roster to be updated for updates to take effect
    """
    if start is None:
        start = datetime.date.today().year - seasons_back
    nba_players = db.session.query(to_id.players).filter(to_id.players.last_season_id > start)
    add_player_start_end(player_list=nba_players, update_roster_prior=update)


def update_standings(max_date=None):
    """
    checks for updated standings. requires schedule to be updated
    """
    update_schedule()  # standings relies on schedule for dates
    if max_date is None:
        max_date = str(db.session.query(func.max(season_model.standings.date)).first()[
                           0])  # .split(' ')[0]  # switch to date instead of year?
    logging.info(f"update_standings(), max_date={max_date})")
    create_standings(max_date)
    # cursor.execute(f'SELECT MAX(date) FROM standings WHERE date > {date} ORDER BY date ASC').fetchone()[0])


def update_all_roster_tables():
    """
    Requires some data in the different roster stats tables. If there is no data, start with create_all_roster_tables
    Runs all update functions from the max year in each table
    """
    for f in ["totals", "per_game", "per_poss", "per_minute"]:
        for p in [True, False]:
            try:
                update_roster_per_x(format=f.upper(), playoffs=p)
            except requests.exceptions.ConnectionError:
                continue


def create_all_roster_tables():
    """
    Creates roster stats in all formats.
    Goal is simply to get tables started, so we use try excepts for the tables since we don't care if they have errors once they make one commit
    Use update_all_roster_tables after using this once because this will
    rewrite data unless you manually give it the maximum year in your tables
    """
    for f in ["totals", "per_game", "per_poss", "per_minute"]:
        for p in [True, False]:
            try:
                create_roster_per_x(format_id=f.upper(), playoffs=p)
            except requests.exceptions.ConnectionError:
                continue
    # move onto the next table after connection error and use update function once all tables have been started


def create_per_all(shuffle=False):
    """
    Creates all formats, playoffs options, and career options for player_per_x, team_per_x, and opponent_per_x
    """
    formats = ["totals", "per_game", "per_poss", "per_minute"]
    playoffs = [True, False]
    career = [True, False]

    if shuffle is True:
        random.shuffle(formats)
        random.shuffle(playoffs)
        random.shuffle(career)

    for f in formats:
        for p in playoffs:
            for c in career:
                create_player_per_x(format=f, playoffs=p, career=c)
        create_team_per_x(start_season=1949, format=f)
        create_opponent_per_x(update_from=1949, format=f)
    create_player_advanced()


def create_box_score_all():
    for p in ["Q1", "Q2", "Q3", "Q4", "H1", "H2", "GAME"]:
        create_box_score_per_x(period=p)
    create_box_score_advanced()


def update_all():
    # update_schedule()  # create before testing
    # update_standings()  # automatically calls update schedule
    while True:  # things that need to run multiple times in case of too many requests
        try:
            update_pbp()
        except:
            pass
        try:
            update_all_box_scores()
        except requests.exceptions.ConnectionError:
            pass
        try:
            update_shot_chart()
        except requests.exceptions.ConnectionError:
            pass
        try:
            update_all_roster_tables()
        except requests.exceptions.ConnectionError:
            pass
        try:
            update_opponent_per_x(format="TOTALS")
        except requests.exceptions.ConnectionError:
            pass
        try:
            update_standings()
            # update_standings(date=cursor.execute("SELECT MAX(date) FROM standings;").fetchone()[0])
        except requests.exceptions.ConnectionError:
            pass
        try:
            update_shot_chart()
        except:
            pass


def starting_build():
    db.create_all()  # create tables
    # insert reference data
    to_id.populate_all()
    create_schedule(1949, datetime.datetime.today().year + 1)
    # start inserting real data
    create_roster()  # build player_name_to_id while creating roster
    create_all_roster_tables()
    create_per_all()
    create_box_score_all()
    create_standings()
    create_shot_chart()
    create_pbp()


if __name__ == '__main__':
    with app.app_context():
        while 1:
            try:
                update_pbp() 
            except:
                pass
            try:
                add_player_start_end(new_players_only=True)
            except:
                pass
            try:
                create_per_all(shuffle=True)
            except:
                pass
            try:
                update_all_box_scores()
            except:
                pass
            try:
                for p in [True, False]:
                    create_player_game_logs(playoffs=p)
            except:
                pass

    # TODO refill tables in postgres db - list tables in starting build to consolidate functions to build tables - finish PBP, finsh box_score_per_x, start game_logs
    # TODO add way to manage career length in player_per_x. (add_player_start_end()) 3907 / 4778 players done - staying at 3907
    # TODO add percentile function

    """    
    team_model.roster_advanced - getting an error calling
    player_model.player_advanced - not getting any response for most player advanced stats
    
    team_model.get_pbp(date, team1, team2) - play by play data for games
    lines: 3672 >> 935
    """
