import flask_sqlalchemy
from flask import Flask

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = "sqlite:////home/nathan/Documents/SportsStatsPython/Interfaces/basketball_reference/NBA_Flask_App.db"  # dialect:///path
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False  # Terminal warning that this will be deprecated soon, so turn it off
sqlite_db = flask_sqlalchemy.SQLAlchemy(app)


app.config['SQLALCHEMY_DATABASE_URI'] = "postgresql://postgres:postgres@localhost/NBA_Stats_Basketball_Reference"  # dialect://user:password@host/db_name
db = flask_sqlalchemy.SQLAlchemy(app)