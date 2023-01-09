from .Base import *
from .to_id import players, team_info, format_to_id, season_to_id
from .season_model import schedule


class player_per_x(db.Model):
    __tablename__ = 'player_per_x'

    id = db.Column(db.Integer, primary_key=True)
    player_id = db.Column(db.Integer, db.ForeignKey(players.id), nullable=False)
    team_id = db.Column(db.Integer, db.ForeignKey(team_info.id), nullable=False)
    format_id = db.Column(db.Integer, db.ForeignKey(format_to_id.id), nullable=False)
    playoffs = db.Column(db.Boolean, nullable=False)
    career = db.Column(db.Boolean, nullable=False)
    age = db.Column(db.Integer)
    ast = db.Column(db.Numeric)
    blk = db.Column(db.Numeric)
    drb = db.Column(db.Numeric)
    fg = db.Column(db.Numeric)
    fga = db.Column(db.Numeric)
    ft = db.Column(db.Numeric)
    fta = db.Column(db.Numeric)
    g = db.Column(db.Integer)
    gs = db.Column(db.Integer)
    league = db.Column(db.String)
    mp = db.Column(db.Numeric)
    orb = db.Column(db.Numeric)
    pf = db.Column(db.Numeric)
    pts = db.Column(db.Numeric)
    season_id = db.Column(db.Integer, db.ForeignKey(season_to_id.id), nullable=False)
    stl = db.Column(db.Numeric)
    tov = db.Column(db.Numeric)
    trb = db.Column(db.Numeric)


class game_log(db.Model):
    __tablename__ = 'player_game_logs'

    game_log_id = db.Column(db.Integer, primary_key=True)
    game_id = db.Column(db.Integer, db.ForeignKey(schedule.game_id))
    team_id = db.Column(db.Integer, db.ForeignKey(team_info.id), nullable=False)
    player_id = db.Column(db.Integer, db.ForeignKey(players.id), nullable=False)
    age = db.Column(db.String)
    ast = db.Column(db.Integer)
    blk = db.Column(db.Integer)
    drb = db.Column(db.Integer)
    fg = db.Column(db.Integer)
    fga = db.Column(db.Integer)
    ft = db.Column(db.Integer)
    fta = db.Column(db.Integer)
    game_score = db.Column(db.Integer)
    game_started = db.Column(db.Integer)
    mp = db.Column(db.Numeric)
    orb = db.Column(db.Integer)
    pf = db.Column(db.Integer)
    pts = db.Column(db.Integer)
    result = db.Column(db.String)
    stl = db.Column(db.Integer)
    turnovers = db.Column(db.Integer)
    trb = db.Column(db.Integer)
    playoffs = db.Column(db.Boolean, nullable=False)


class player_advanced(db.Model):
    __tablename__ = 'player_advanced'

    id = db.Column(db.Integer, primary_key=True)
    player_id = db.Column(db.Integer, db.ForeignKey(players.id))
    career = db.Column(db.Boolean, nullable=False)
    playoffs = db.Column(db.Boolean, nullable=False)
    season_id = db.Column(db.Integer, db.ForeignKey(season_to_id.id), nullable=False)
    PER = db.Column(db.Numeric)
    TS_pct = db.Column(db.Numeric)
    PAr_3 = db.Column(db.Numeric)
    FTr = db.Column(db.Numeric)
    ORB_pct = db.Column(db.Numeric)
    DRB_pct = db.Column(db.Numeric)
    TRB_pct = db.Column(db.Numeric)
    AST_pct = db.Column(db.Numeric)
    STL_pct = db.Column(db.Numeric)
    BLK_pct = db.Column(db.Numeric)
    USG_pct = db.Column(db.Numeric)
    OWS = db.Column(db.Numeric)
    DWS = db.Column(db.Numeric)
    WS = db.Column(db.Numeric)
    WS_per_48 = db.Column(db.Numeric)
    OBPM = db.Column(db.Numeric)
    DBPM = db.Column(db.Numeric)
    BPM = db.Column(db.Numeric)
    VORP = db.Column(db.Numeric)


class roster_per_x(db.Model):
    __tablename__ = 'roster_per_x'

    id = db.Column(db.Integer, primary_key=True)
    ast = db.Column(db.Numeric)
    blk = db.Column(db.Numeric)
    drb = db.Column(db.Numeric)
    fg = db.Column(db.Numeric)
    fga = db.Column(db.Numeric)
    ft = db.Column(db.Numeric)
    fta = db.Column(db.Numeric)
    g = db.Column(db.Integer)
    gs = db.Column(db.Integer)
    mp = db.Column(db.Numeric)
    orb = db.Column(db.Numeric)
    pf = db.Column(db.Numeric)
    player_id = db.Column(db.Integer, db.ForeignKey(players.id), nullable=False)  # foreign key
    pts = db.Column(db.Numeric)
    season_id = db.Column(db.Integer, db.ForeignKey(season_to_id.id), nullable=False)
    stl = db.Column(db.Numeric)
    format_id = db.Column(db.Integer, db.ForeignKey(format_to_id.id), nullable=False)
    team_id = db.Column(db.Integer, db.ForeignKey(team_info.id), nullable=False)  # foreign key
    tov = db.Column(db.Numeric)
    trb = db.Column(db.Numeric)
    playoffs = db.Column(db.Boolean, nullable=False)

    # class roster_advanced(db.Model):
    # throws UnboundLocalError for unassigned 'selector


if __name__ == '__main__':
    db.create_all()
