from .to_id import players, team_info, season_to_id
from .season_model import schedule
from .Base import *

class shot_chart(db.Model):
    __tablename__ = 'shot_chart'
    __table_args__ = {'extend_existing': True}

    shot_id = db.Column(db.Integer, primary_key=True)
    shooter_id = db.Column(db.Integer, db.ForeignKey(players.id), nullable=False)
    team_id = db.Column(db.Integer, db.ForeignKey(team_info.id), nullable=False)
    game_id = db.Column(db.Integer, db.ForeignKey(schedule.game_id), nullable=False)
    season_id = db.Column(db.Integer, db.ForeignKey(season_to_id.id), nullable=False)
    quarter = db.Column(db.Integer)
    distance = db.Column(db.Numeric)
    make = db.Column(db.Boolean)
    time_remaining=db.Column(db.Numeric)
    value = db.Column(db.Integer)
    x = db.Column(db.Numeric)
    y = db.Column(db.Numeric)


if __name__ == '__main__':
    db.create_all()
