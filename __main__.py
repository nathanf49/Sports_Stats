# run with $ python3 . from this directory
from Models.Base import db
from Models import to_id
from flask_API import app

if __name__ == "__main__":
    db.create_all()
    to_id.populate_all()
    app.run()
