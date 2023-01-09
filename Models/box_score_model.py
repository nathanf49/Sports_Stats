from .to_id import team_info, format_to_id, players
from .season_model import schedule
from .Base import *


class box_score_per_x(db.Model):
    __tablename__ = "box_score_per_x"
    __table_args__ = {'extend_existing': True}

    id = db.Column(db.Integer, primary_key=True)
    game_id = db.Column(db.Integer, db.ForeignKey(schedule.game_id), nullable=False)
    player_id = db.Column(db.Integer, db.ForeignKey(players.id), nullable=False)
    team_id = db.Column(db.Integer, db.ForeignKey(team_info.id), nullable=False)
    ast = db.Column(db.Integer)
    blk = db.Column(db.Integer)
    fg = db.Column(db.Integer)
    fga = db.Column(db.Integer)
    fg_3p = db.Column(db.Integer)
    fg_3pa = db.Column(db.Integer)
    ft = db.Column(db.Integer)
    fta = db.Column(db.Integer)
    mp = db.Column(db.Numeric)
    orb = db.Column(db.Integer)
    drb = db.Column(db.Integer)
    trb = db.Column(db.Integer)
    pf = db.Column(db.Integer)
    pts = db.Column(db.Integer)
    stl = db.Column(db.Integer)
    tov = db.Column(db.Integer)
    plus_minus = db.Column(db.String)
    format_id = db.Column(db.Integer, db.ForeignKey(format_to_id.id), nullable=False)
    playoffs = db.Column(db.Boolean)


class box_score_advanced(db.Model):
    __tablename__ = 'box_score_advanced'
    __table_args__ = {'extend_existing': True}

    id = db.Column(db.Integer, primary_key=True)
    game_id = db.Column(db.Integer, db.ForeignKey(schedule.game_id), nullable=False)
    player_id = db.Column(db.Integer, db.ForeignKey(players.id), nullable=False)
    team_id = db.Column(db.Integer, db.ForeignKey(team_info.id), nullable=False)
    format_id = db.Column(db.Integer, db.ForeignKey(format_to_id.id), nullable=False)
    bpm = db.Column(db.Float)
    drtg = db.Column(db.Float)
    ftr = db.Column(db.Float)
    mp = db.Column(db.Float)
    ortg = db.Column(db.Float)


if __name__ == '__main__':
    db.create_all()
    # test()
