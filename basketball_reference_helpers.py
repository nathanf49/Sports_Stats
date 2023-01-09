import datetime
from flask import Flask
import flask_sqlalchemy
from sqlalchemy import func, create_engine
from Models import player_model, team_model, season_model, to_id
import logging

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = "postgresql://postgres:postgres@localhost/NBA_Stats_Basketball_Reference"  # "sqlite:////home/nathan/Documents/SportsStatsPython/Interfaces/basketball_reference/NBA_Flask_App.db"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False  # Terminal warning that this will be deprecated soon
db = flask_sqlalchemy.SQLAlchemy(app)
engine = create_engine(app.config['SQLALCHEMY_DATABASE_URI'])
logging.basicConfig(filename='CRUD_helpers.log', level=logging.INFO, format=f' {datetime.datetime.now()} - %(levelname)s - %(message)s')


def get_team(team):
    """
    team can be abbreviation, full name, city, mascot or id
    """
    if type(team) is int:
        return db.session.query(to_id.team_info).filter_by(id=team).first()  # find team by id
    elif type(team) is str:  # find team by full name
        team = team.upper()
        if "/" in team:  # Avoid New Orleans/Oklahoma City Hornets not matching
            team = team.replace("/OKLAHOMA CITY", "")
        elif "LA " in team:  # team objects are stored as Los Angeles
            team = team.replace("LA ", "LOS ANGELES ")
        if len(team) == 3:  # find team by abbreviation
            return db.session.query(to_id.team_info).filter_by(abbreviation=team).first()
        team_object = db.session.query(to_id.team_info).filter_by(full_name=team).first()
        if team_object is not None:
            return team_object
        team_object = db.session.query(to_id.team_info).filter_by(city=team).first()  # find team by city
        if team_object is not None:
            return team_object
        team_object = db.session.query(to_id.team_info).filter_by(mascot=team).first()  # find team by mascot
        if team_object is not None:
            return team_object
        logging.debug(f"Could not find team: {team}")
        return None
    else:
        logging.debug('Please input team as a string or integer')
        return None  # return None if team is not in table


def get_player(name):
    """
    returns player id
    Can accept name or id
    Only adds missing players when a string is passed
    """
    if type(name) is to_id.players:  # name is already a player. Likely a mistake so just return it
        return name

    elif type(name) is str:
        player = db.session.query(to_id.players).filter_by(name=name).first()

        if player is None:
            with db.session.no_autoflush:
                db.session.add(
                    to_id.players(name=name))
                db.session.commit()

                player = db.session.query(to_id.players).filter_by(name=name).first()
    elif type(name) is int:
        player = db.session.query(to_id.players).filter_by(id=name).first()

    else:
        raise TypeError("Player name must be a string or integer")
    return player


def get_all_players_per_x(team=None, format=None):
    """
    returns players from player_per_x
    team will return only players from that team. It can be abbreviation, full name, id
    """
    if format is not None:  # convert format to object with format name and id
        format = get_format(format)
    if format is None and team is None:
        return player_model.player_per_x.all()
    elif team is None:
        return player_model.player_per_x.filter_by(format=format)
    elif format is None:
        return player_model.player_per_x.filter_by(team_id=get_team(team))
    else:
        return player_model.player_per_x.filter_by(team_id=get_team(team), format_id=format.id)


def get_game(date, team1=None, team2=None):
    """
    If teams are not submitted, use date as game_id
    Date should be in format YYYY-MM-DD
    teams can be in any format
    """
    if type(date) is int and team1 is None and team2 is None:
        game_id = db.session.query(season_model.schedule).filter_by(game_id=date).first()
    else:
        team1 = get_team(team1).id
        team2 = get_team(team2).id

        game_id = db.session.query(season_model.schedule).filter_by(date=date, home_id=team1, visitor_id=team2).first()
        if game_id is None:  # switch teams
            game_id = db.session.query(season_model.schedule).filter_by(date=date, home_id=team2, visitor_id=team1).first()

    return game_id


def get_all_game_dates(gameID=None):
    """
    returns dates for all games played in schedule
    returns each date only once
    if game ID is not None, returns all games after that gameID
    """
    if gameID is None:
        games = db.session.query(season_model.schedule.date).distinct(season_model.schedule.date).all()
        # query = 'SELECT DISTINCT(date) FROM schedule;'
    else:
        games = db.session.query(season_model.schedule.date).filter(season_model.schedule.game_id >= gameID).distinct(season_model.schedule.date).all()
        # query = 'SELECT DISTINCT(date) FROM schedule WHERE game_id >=' + str(gameID) + ";"
    games = [i[0] for i in games]
    return games


def mp_handler(mp):
    if type(mp) is str:
        if mp.replace(":", "").isnumeric():
            return float(mp.replace(":", "."))
        else:
            return None

    elif type(mp) is float or type(mp) is int:
        return float(mp)

    else:
        return None


def get_all_teams():  # returns team objects
    return db.session.query(team_model.team_info).all()


def int_cast(num):  # helps with errors for missing data
    try:
        num = int(num)
    except (ValueError, AttributeError):
        num = None
    return num


def play_breakdown(fields):
    """
    Takes a single line of a dataframe from pbp model to find which team was acting for the play
    Returns a dictionary with data for the play with the acting team identified
    """
    if type(fields.iloc[0][2]) is str:
        return {'ACTION': fields.iloc[0][2],
                'ACTING TEAM SCORE': fields.iloc[0][4],
                'OPPONENT SCORE': fields.iloc[0][5],
                'ACTING TEAM': get_team(fields.keys()[4][:-6]).id,
                'OPPONENT': get_team(fields.keys()[5][:-6]).id}
    else:
        return {'ACTION': fields.iloc[0][3],
                'ACTING TEAM SCORE': fields.iloc[0][5],
                'OPPONENT SCORE': fields.iloc[0][4],
                'ACTING TEAM': get_team(fields.keys()[5][:-6]).id,
                'OPPONENT': get_team(fields.keys()[4][:-6]).id}


def check_column(data, col, index=None):
    """
    This checks dataframes to see if a key exists. If it does not, return None. If it does, return requested data
    Inputs are the dataframe you're looking at, the key/column you're looking at as a string and the index is optional
    """
    if col in data.keys():
        if index is None:
            return data[col]
        else:
            return data[col][index]
    else:
        return None


def get_career_seasons(player):
    """
    Returns a list with the first season a player played and the last
    player can be a name or id
    """
    return list(db.session.query(func.min(team_model.roster.season_id), func.max(team_model.roster.season_id)).filter_by(player_id=get_player(player).id).first())
    # return cursor.execute(f"SELECT MIN(season), MAX(season) FROM roster WHERE name ='{player_name[0]}' ").fetchone()


def get_format(format):
    """
    Converts format for per_x tables
    """

    if type(format) is int:
        conversion = to_id.format_to_id.query.filter_by(format_id=format).first()

    elif type(format) is str:
        format.replace("Q", "QUARTER_")
        format.replace("H", "HALF_")
        format.replace(" ", "_")
        if format.isnumeric():
            conversion = to_id.format_to_id.query.filter_by(format_id=int(format)).first()
        else:
            conversion = to_id.format_to_id.query.filter_by(format=format.upper()).first()

    else:
        raise TypeError("Submit format as str or int")

    return conversion


def datetime_to_date_str(dt):
    logging.info(f"Converting datetime: {str(dt)} to date")
    if type(dt) is not datetime.datetime:
        if type(dt[0]) is datetime.datetime:
            dt = dt[0]
        else:
            raise TypeError("Input to datetime_to_date_str should be of type datetime")

    return str(dt.date())


def get_season_id_from_date(year, month=None):
    """
    Date should be passed in as 2 ints, but should work with datetime strings too
    """
    if type(year) is str and month is None:
        month = year.split(" ")[0].split("-")[1]
        year = year.split(" ")[0].split("-")[0]

    if int(month) >= 9:
        return int(year) + 1
    else:
        return int(year)


def get_locale(locale):
    if type(locale) is int:
        ret = db.session.query(to_id.locale_to_id).filter_by(id=locale).first()
    elif type(locale) is str:
        if locale.isnumeric():
            ret = db.session.query(to_id.locale_to_id).filter_by(id=int(locale)).first()
        else:
            if "DIVISION" in locale.upper() or "CONF" in locale.upper():
                locale = locale.upper().replace("DIVISION", "").replace("CONFERENCE", "").replace(" ", "").replace("CONF", "").replace("_", "")
            ret = db.session.query(to_id.locale_to_id).filter_by(division=locale.upper()).first()
            if ret is None:
                ret = db.session.query(to_id.locale_to_id).filter_by(division=None, conference=locale).first()
    else:
        raise TypeError("locale should be the name of a locale, a numeric string or an integer")

    if ret is None:
        raise Warning("Locale not found. ids should be between 1 and 8 inclusive")

    return ret
