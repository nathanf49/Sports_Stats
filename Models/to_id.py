from .Base import *
import json


class locale_to_id(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    division = db.Column(db.String)
    conference = db.Column(db.String)


class format_to_id(db.Model):
    __tablename__ = "format_to_id"

    id = db.Column(db.Integer, primary_key=True)
    format = db.Column(db.String)


class season_to_id(db.Model):  # gives each season an id since seasons overlap years
    __tablename__ = 'season_to_id'
    __table_args__ = {'extend_existing': True}

    id = db.Column(db.Integer, primary_key=True)
    season = db.Column(db.String)


class players(db.Model):
    __tablename__ = 'players'
    __table_args__ = {'extend_existing': True}

    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String, nullable=False)
    birth_date = db.Column(db.DateTime)
    college = db.Column(db.String)
    height = db.Column(db.String)
    nationality = db.Column(db.String)
    first_season_id = db.Column(db.Integer, db.ForeignKey(season_to_id.id))
    last_season_id = db.Column(db.Integer, db.ForeignKey(season_to_id.id))


class position_to_id(db.Model):
    __tablename__ = 'position_to_id'

    id = db.Column(db.Integer, primary_key=True)
    position = db.Column(db.String, unique=True)
    position_id = db.Column(db.Numeric, unique=True)


def position_converter(position):
    """
    position should be a string or numeric
    positions are stored as integers or in the case of players with multiple positions as floats
    ex F-C would be 4.5
    """
    if str(position).isnumeric():
        return str(position).replace('4.5', 'F-C').replace('2.3', 'G-F').replace('5.4', 'C-F').replace('1', 'PG').replace('3.4', 'F').replace('1.2', 'G').replace('3.2', 'F-G').replace('2', 'SG').replace('3', 'SF').replace('4', 'PF').replace('5', 'C')
    else:
        return float(str(position).replace('G-F', '2.3').replace('F-C', '4.5').replace('C-F', '5.4').replace('F-G', '3.2').replace('PG', '1').replace('SG', '2').replace('SF', '3').replace('PF', '4').replace('C', '5').replace('F', '3.4').replace('G', '1.2'))


class team_info(db.Model):
    __tablename__ = 'team_info'
    __table_args__ = {'extend_existing': True}

    id = db.Column(db.Integer, primary_key=True)
    abbreviation = db.Column(db.String(3), nullable=False)  # team abbreviation
    full_name = db.Column(db.String)  # Full team name
    city = db.Column(db.String)
    mascot = db.Column(db.String)


def populate_team_to_id():  # created for 30 current teams and Sonics
    with open('/home/nathan/Documents/SportsStatsPython/Interfaces/basketball_reference/Models/teamID.json', 'r') as file:
        info = json.load(file)
        pass
    for team in info.keys():
        exists = db.session.query(team_info).filter_by(
            abbreviation=team).first() is not None  # checks if team is already in table
        if exists:
            pass  # team is already in table. No need to add
        else:
            db.session.add(team_info(abbreviation=team.upper(), full_name=info[team][0].upper(), city=info[team][1].upper(),
                                     mascot=info[team][2].upper()))
    db.session.commit()


def populate_season_to_id(start=0, end=3000):
    """
    Maps seasons to season_id since NBA seasons overlap multiple years.
    season_id is the year the season ended
    ex 2021-22 seasons season_id is 2022
    Default start is 0 to ease readability
    """
    for season in range(start, end):  # always stay in current or next season
        end_part = str(int(str(season)) + 1)[-2:]
        if len(end_part) == 1:
            end_part = '0' + end_part
        nba_season = str(season) + '-' + end_part
        exists = db.session.query(season_to_id).filter_by(season=nba_season).first() is not None

        if exists:
            continue
        else:
            db.session.add(season_to_id(season=nba_season))
    db.session.commit()


def populate_position_to_id():
    positions = ['PG', "G", "SG", "G-F", "F-G", "SF", "F", "PF", "F-C", "C-F", "C"]
    for p in positions:
        exists = db.session.query(position_to_id).filter_by(position=p).first() is not None
        if exists:
            continue
        else:
            db.session.add(position_to_id(position=p, position_id=position_converter(p)))
    db.session.commit()


def populate_format():
    formats = ["QUARTER_1", "QUARTER_2", "QUARTER_3", "QUARTER_4", "HALF_1", "HALF_2", "GAME", "TOTALS", "PER_GAME",
               "PER_MINUTE", "PER_POSS"]
    for f in formats:
        exists = db.session.query(format_to_id).filter_by(format=f).first() is not None
        if exists:
            continue
        else:
            db.session.add(format_to_id(format=f))
    db.session.commit()


def populate_locale():
    divisions = {"ATLANTIC": "EASTERN",
                 "CENTRAL": "EASTERN",
                 "SOUTHEAST": "EASTERN",
                 "NORTHWEST": "WESTERN",
                 "PACIFIC": "WESTERN",
                 "SOUTHWEST": "WESTERN",
                 "MIDWEST": "WESTERN"}
    conferences = {"EASTERN": None,
                   "WESTERN": None}

    for d in divisions.keys():
        exists = db.session.query(locale_to_id).filter_by(division=d, conference=divisions[d]).first() is not None
        if exists:
            continue
        else:
            db.session.add(locale_to_id(division=d, conference=divisions[d]))
    for c in conferences.keys():
        exists = db.session.query(locale_to_id).filter_by(division=None, conference=c).first() is not None
        if exists:
            continue
        else:
            db.session.add(locale_to_id(division=None, conference=c))
    db.session.commit()


def populate_all():
    db.create_all()
    populate_locale()
    populate_format()
    populate_position_to_id()
    populate_season_to_id()
    populate_team_to_id()


if __name__ == "__main__":
    populate_all()
