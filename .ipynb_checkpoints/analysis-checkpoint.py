import scipy.stats

from Models import team_model, to_id, player_model, shot_chart_model, season_model, box_score_model
from Models.Base import db
import basketball_reference_helpers as helpers
from sqlalchemy import create_engine, inspect
from scipy import stats

engine = create_engine("postgresql://postgres:postgres@localhost/NBA_Stats_Basketball_Reference")
inspector = inspect(engine)
def percentile(name, field):
    p = helpers.get_player(name)
    format = helpers.get_format("PER_GAME")
    single = db.session.query(player_model.player_per_x).filter_by(player_id=p.id, format_id=format.id).first()
    all = db.session.query(player_model.player_per_x).filter_by(format_id=format.id).all()
    #scipy.stats.percentileofscore([float(i.pts) if i is not None else 0 for i in all], single.pts)  # scipy doesn't like that isnan is present
    # tables = {"tables": inspector.get_table_names()} # collect available tables
    # columns = [i['name'] for i in inspector.get_columns(table)] # collect fields available in each table

if __name__ == "__main__":
    percentile(1,"")