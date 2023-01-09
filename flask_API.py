# test from terminal with curl + API_URL
import flask
import flask_sqlalchemy
from sqlalchemy import create_engine, inspect
import logging
import datetime
import os
from Models import to_id
import json

app = flask.Flask(__name__)
app.secret_key = 'Secret Key'
app.config['SQLALCHEMY_DATABASE_URI'] = "postgresql://postgres:postgres@localhost/NBA_Stats_Basketball_Reference"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False  # Terminal warning that this will be deprecated soon
db = flask_sqlalchemy.SQLAlchemy(app)
engine = create_engine(app.config['SQLALCHEMY_DATABASE_URI'])
inspector = inspect(engine)


@app.route("/check_connection")
def check_connection():  # test if database is returning anything
    return json.dumps({"Connected": db.session.query(to_id.team_info).all() is not None})


@app.route("/tables")
def available_tables():
    tables = {"tables": inspector.get_table_names()}
    log(f"Returning tables")
    return json.dumps(tables)


@app.route("/query/<string:table>")
def available_fields(table):
    columns = inspector.get_columns(table)
    columns = [i['name'] for i in columns]
    log(f"Returning columns for {table}")
    return json.dumps({f"Columns in {table}": columns})


@app.route("/query/<string:table>/players")
def available_players(table):
    players = db.session.query(to_id.players.name).distinct().all() # TODO checking distinct players, not input table
    players = [i[0] for i in players]
    log(f"Returning players for {table}")
    return json.dumps({"table": table, "players": players})


def connect_logger(filename=None):
    log_level = logging.INFO
    for handler in app.logger.handlers:
        app.logger.removeHandler(handler)

    root = os.path.dirname(os.path.abspath(__file__))
    logdir = os.path.join(root, '../logs')
    if not os.path.exists(logdir):
        os.mkdir(logdir)
    if filename is not None:
        log_file = os.path.join(logdir, filename)
    else:
        log_file = os.path.join(logdir, 'flask_API.log')
    handler = logging.FileHandler(log_file)
    handler.setLevel(log_level)
    app.logger.addHandler(handler)
    app.logger.setLevel(log_level)


def log(msg, log_level='info'):
    msg = str(datetime.datetime.now()) + ' - ' + msg
    if log_level.lower() == 'warning':
        app.logger.warning(msg)
    elif log_level.lower() == 'error':
        app.logger.error(msg)
    else:
        app.logger.info(msg)


def create_app():
    db.create_all()  # Make sure Users table and db exist
    connect_logger()
    log(f'App started')
    app.run(debug=True, port=5000)
    log(f'App shutdown')


if __name__ == "__main__":
    #available_fields('team_info')
    available_players('roster')
    # available_tables()
    # create_app()