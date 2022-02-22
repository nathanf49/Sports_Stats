import peewee

db = peewee.SqliteDatabase('/home/nathan/Documents/pythonPractice/SportsStatsPython/peeWee_SQLite/player_game_log.db')

class player_game_log(peewee.Model):
    SEASON_ID = peewee.CharField(null=True)
    Player_Name = peewee.CharField(null=True)
    Player_ID = peewee.IntegerField(null=True)
    Game_ID = peewee.CharField(null=True)
    GAME_DATE = peewee.CharField(null=True)
    MATCHUP = peewee.CharField(null=True)
    WL = peewee.CharField(null=True)
    MIN = peewee.IntegerField(null=True)
    FGM = peewee.IntegerField(null=True)
    FGA = peewee.IntegerField(null=True)
    FG_PCT = peewee.FloatField(null=True)
    FG3M = peewee.IntegerField(null=True)
    FG3A = peewee.IntegerField(null=True)
    FG3_PCT = peewee.FloatField(null=True)
    FTM = peewee.IntegerField(null=True)
    FTA = peewee.IntegerField(null=True)
    FT_PCT = peewee.FloatField(null=True)
    OREB = peewee.IntegerField(null=True)
    DREB = peewee.IntegerField(null=True)
    REB = peewee.IntegerField(null=True)
    AST = peewee.IntegerField(null=True)
    STL = peewee.IntegerField(null=True)
    BLK = peewee.IntegerField(null=True)
    TOV = peewee.IntegerField(null=True)
    PF = peewee.IntegerField(null=True)
    PTS = peewee.IntegerField(null=True)
    PLUS_MINUS = peewee.IntegerField(null=True)
    VIDEO_AVAILABLE = peewee.IntegerField(null=True)

    class Meta:
        database = db
        db_table = 'Player Game Logs'


def init_database():
    db.connect()
    db.create_tables([player_game_log], safe=True)
    db.close()
