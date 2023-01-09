from .to_id import team_info, season_to_id, locale_to_id
from .Base import *


class schedule(db.Model):  # updated, but needs to be dropped and rebuilt
    __tablename__ = 'schedule'
    __table_args__ = {'extend_existing': True}

    game_id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.DateTime)
    season_id = db.Column(db.Integer, db.ForeignKey(season_to_id.id))
    home_id = db.Column(db.Integer)
    visitor_id = db.Column(db.Integer, db.ForeignKey(team_info.id))
    home_pts = db.Column(db.Integer)
    visitor_pts = db.Column(db.Integer)


class standings(db.Model):
    __tablename__ = 'standings'
    __table_args__ = {'extend_existing': True}

    id = db.Column(db.Integer, primary_key=True)
    gb = db.Column(db.Integer)
    w = db.Column(db.Integer)
    l = db.Column(db.Integer)
    locale_id = db.Column(db.Integer, db.ForeignKey(locale_to_id.id))
    team_id = db.Column(db.Integer, db.ForeignKey(team_info.id))
    date = db.Column(db.String)


if __name__ == '__main__':
    db.create_all()
