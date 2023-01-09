import datetime
from flask import Flask
import flask_sqlalchemy
from sqlalchemy import func, create_engine
from Models import player_model, team_model, season_model, to_id

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = "postgresql://postgres:postgres@localhost/NBA_Stats_Basketball_Reference"  # "sqlite:////home/nathan/Documents/SportsStatsPython/Interfaces/basketball_reference/NBA_Flask_App.db"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False  # Terminal warning that this will be deprecated soon
db = flask_sqlalchemy.SQLAlchemy(app)
engine = create_engine(app.config['SQLALCHEMY_DATABASE_URI'])


def get_team(team):
    """
    team can be abbreviation, full name, city, mascot or id
    """
    if type(team) is int:
        return db.session.query(to_id.team_info).filter_by(id=team).first()  # find team by id
    elif type(team) is str:  # find team by full name
        if "/" in team:  # Avoid New Orleans/Oklahoma City Hornets not matching
            team = team.replace("/Oklahoma City", "")
        if len(team) == 3:  # find team by abbreviation
            return db.session.query(to_id.team_info).filter_by(abbreviation=team).first()
        team = db.session.query(to_id.team_info).filter_by(full_name=team).first()
        if team is not None:
            return team
        team = db.session.query(to_id.team_info).filter_by(city=team).first()  # find team by city
        if team is not None:
            return team
        team = db.session.query(to_id.team_info).filter_by(mascot=team).first()  # find team by mascot
        if team is not None:
            return team
        print(f"Could not find team: {team}")
        return None
    else:
        print('Please input team as a string or integer')
        return None  # return None if team is not in table


def get_player(name):
    """
    returns player id
    Can accept name or id
    Only adds missing players when a string is passed
    """
    if type(name) is str:
        player = db.session.query(to_id.player).filter_by(name=name).first()

        if player is None:
            with db.session.no_autoflush:
                db.session.add(
                    to_id.player(name=name))
                db.session.commit()

                player = db.session.query(to_id.player).filter_by(name=name).first()
    elif type(name) is int:
        player = db.session.query(to_id.player).filter_by(id=name).first()

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
    Returns a dictionary with the action, score of the acting team, score of the opponent, name of the acting team, and name of the opponent in that order
    """
    if type(fields.iloc[0][2]) is str:
        return {'action': fields.iloc[0][2],
                'acting team score': fields.iloc[0][4],
                'opponent score': fields.iloc[0][5],
                'acting team': fields.keys()[4][:-6],
                'opponent': fields.keys()[5][:-6]}
    else:
        return {'action': fields.iloc[0][3],
                'acting team score': fields.iloc[0][5],
                'opponent score': fields.iloc[0][4],
                'acting team': fields.keys()[5][:-6],
                'opponent': fields.keys()[4][:-6]}


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
    if type(dt) is not datetime.datetime:
        if type(dt[0]) is datetime.datetime:
            dt = dt[0]
        else:
            raise TypeError("Input to datetime_to_date_str should be of type datetime")
    year = str(dt.year)
    month = str(dt.month)
    day = str(dt.day)
    if len(month) == 1:
        month = '0' + month

    if len(day) == 1:
        day = '0' + day

    return f"{year}-{month}-{day}"


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


def test_get_season_id_from_date():
    assert get_season_id_from_date(year=2020, month=10) == 2021
    assert get_season_id_from_date('2020-10-06') == 2021
    assert get_season_id_from_date('1985', '1') == 1985
    assert get_season_id_from_date(1991, 4) == 1991
    assert get_season_id_from_date('1991-04-20') == 1991
    assert get_season_id_from_date(datetime_to_date_str(datetime.datetime(1987, 10, 6))) == 1988


def test_datetime_to_date_str():
    assert datetime_to_date_str(datetime.datetime(year=1997, month=10, day=6)) == '1997-10-06'
    assert datetime_to_date_str(datetime.datetime(1998, 1, 14)) == "1998-01-14"
    assert datetime_to_date_str(datetime.datetime(2022, 12, 6)) == "2022-12-06"


def test_get_division():
    assert get_locale('1').locale == "ATLANTIC"
    assert get_locale("Atlantic").id == 1
    assert get_locale(1).locale == "ATLANTIC"
    assert get_locale('9') is None
    assert get_locale('atlantic').id == 1
    assert get_locale(10) is None


def test_format_converter():
    assert get_format(1) == "TOTALS"
    assert get_format(2) == "PER_GAME"
    assert get_format(3) == "PER_MINUTE"
    assert get_format(4) == "PER_POSS"
    assert get_format("totals") == 1
    assert get_format("PER_GAME") == 2
    assert get_format("not valid") is None


def test_mp_handler():
    assert mp_handler('7:36') == 7.36  # test str
    assert mp_handler(7.36) == 7.36  # test float
    assert mp_handler(7) == 7  # test int
    assert mp_handler(None) is None


def test_convert_format():
    assert get_format('quarter_1').id == 1  # test with id
    assert get_format(1).format == "QUARTER_1"  # test with format name
    assert get_format("TOTALS").id == 8
    assert get_format("Nathan") is None  # test with fake format
    assert get_format("Q1").id == 1
    assert get_format(7).format == "GAME"


def test_get_career_seasons():
    assert get_career_seasons("Michael Jordan") == [1985, 2003]  # test with player name
    assert get_career_seasons(get_player("Michael Jordan").id) == [1985, 2003]  # test with player id
    assert get_career_seasons("Kobe Bryant") == [1997, 2016]
    assert get_career_seasons("LeBron James") == [2004, get_season_id_from_date(year=datetime.datetime.now().year, month=datetime.datetime.now().month)]  # test with active player
    assert get_career_seasons("Nathan Frank") == [None, None]  # test this when shot chart finishes. Should return None


if __name__ == "__main__":
    test_mp_handler()
