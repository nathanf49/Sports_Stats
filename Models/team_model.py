from .to_id import players, format_to_id, season_to_id, team_info
from .Base import *



class roster(db.Model):
    __tablename__ = 'roster'
    __table_args__ = {'extend_existing': True}

    id = db.Column(db.Integer, primary_key=True)
    team_id = db.Column(db.Integer, db.ForeignKey(team_info.id))  # foreign key
    season_id = db.Column(db.Integer,db.ForeignKey(season_to_id.id), nullable=False)
    number = db.Column(db.Integer, nullable=True)  # some players don't have numbers on basketball reference for some reason
    player_id = db.Column(db.Integer, db.ForeignKey(players.id))
    weight = db.Column(db.Integer, nullable=False)
    position_id = db.Column(db.String, nullable=False)


class team_per_x(db.Model):
    __tablename__ = 'team_per_x'
    __table_args__ = {'extend_existing': True}

    id = db.Column(db.Integer, primary_key=True)
    team_id = db.Column(db.Integer, db.ForeignKey(team_info.id), nullable=False)  # foreign key
    ast = db.Column(db.Integer)
    blk = db.Column(db.Integer)
    drb = db.Column(db.Integer)
    fg = db.Column(db.Integer)
    fga = db.Column(db.Integer)
    ft = db.Column(db.Integer)
    fta = db.Column(db.Integer)
    g = db.Column(db.Integer)
    mp = db.Column(db.Integer)
    orb = db.Column(db.Integer)
    pf = db.Column(db.Integer)
    pts = db.Column(db.Integer)
    season_id = db.Column(db.Integer, db.ForeignKey(season_to_id.id), nullable=False)
    stl = db.Column(db.Integer)
    tov = db.Column(db.Integer)
    trb = db.Column(db.Integer)
    format_id = db.Column(db.Integer, db.ForeignKey(format_to_id.id), nullable=False)


class opponent_per_x(db.Model):
    __tablename__ = 'opponent_per_x'
    __table_args__ = {'extend_existing': True}

    id = db.Column(db.Integer, primary_key=True)
    team_id = db.Column(db.Integer, db.ForeignKey(team_info.id))  # foreign key, not opponent
    opp_2p = db.Column(db.Float)
    opp_2pa = db.Column(db.Float)
    opp_3p = db.Column(db.Float)
    opp_3pa = db.Column(db.Float)
    opp_ast = db.Column(db.Float)
    opp_blk = db.Column(db.Float)
    opp_drb = db.Column(db.Float)
    opp_fg = db.Column(db.Float)
    opp_ft = db.Column(db.Float)
    opp_fga = db.Column(db.Float)
    opp_fta = db.Column(db.Float)
    opp_g = db.Column(db.Integer)
    opp_mp = db.Column(db.Float)
    opp_orb = db.Column(db.Float)
    opp_pf = db.Column(db.Float)
    opp_pts = db.Column(db.Float)
    opp_stl = db.Column(db.Float)
    opp_tov = db.Column(db.Float)
    opp_trb = db.Column(db.Float)
    format_id = db.Column(db.Integer, db.ForeignKey(format_to_id.id), nullable=False)
    season_id = db.Column(db.Integer, db.ForeignKey(season_to_id.id), nullable=False)


# class roster_advanced(db.Model):
    # throws UnboundLocalError for unassigned 'selector

# check return and fill in


class team_misc(db.Model):
    __tablename__ = 'team_misc'
    __table_args__ = {'extend_existing': True}

    id = db.Column(db.Integer, primary_key=True)
    arena = db.Column(db.String, nullable=False)
    attendance = db.Column(db.Integer, nullable=False)
    attendance_per_game = db.Column(db.Integer, nullable=False)
    ts = db.Column(db.Float, nullable=False)
    par_3 = db.Column(db.Float, nullable=False)
    efg_pct = db.Column(db.Float, nullable=False)
    tov_pct = db.Column(db.Float, nullable=False)
    orb_pct = db.Column(db.Float, nullable=False)
    dreb_pct = db.Column(db.Float, nullable=False)
    ft_fga = db.Column(db.Float, nullable=False)
    drtg = db.Column(db.Float, nullable=False)
    ftr = db.Column(db.Float, nullable=False)
    w = db.Column(db.Integer, nullable=False)
    l = db.Column(db.Integer, nullable=False)
    mov = db.Column(db.Float, nullable=False)
    nrtg = db.Column(db.Float, nullable=False)
    ortg = db.Column(db.Float, nullable=False)
    pace = db.Column(db.Float, nullable=False)
    pl = db.Column(db.Integer, nullable=False)
    pw = db.Column(db.Integer, nullable=False)
    season_id = db.Column(db.Integer, db.ForeignKey(season_to_id.id), nullable=False)
    sos = db.Column(db.Float, nullable=False)
    srs = db.Column(db.Float, nullable=False)
    team_id = db.Column(db.Integer, db.ForeignKey(team_info.id))  # foreign key


class play_by_play(db.Model):
    __tablename__ = 'pbp'
    __table_args__ = {'extend_existing': True}

    play_id = db.Column(db.Integer, primary_key=True)
    game_id = db.Column(db.Integer, db.ForeignKey(season_to_id.id))
    quarter = db.Column(db.Integer)
    time_remaining = db.Column(db.String)
    action = db.Column(db.String)
    acting_team_id = db.Column(db.Integer, db.ForeignKey(team_info.id))
    opponent_team_id = db.Column(db.Integer, db.ForeignKey(team_info.id))
    acting_team_score = db.Column(db.Integer)
    opponent_score = db.Column(db.Integer)


"""
def test():
    x = ()
    db.session.add(x)
    db.session.commit()"""

if __name__ == '__main__':
    db.create_all()
    # test()
