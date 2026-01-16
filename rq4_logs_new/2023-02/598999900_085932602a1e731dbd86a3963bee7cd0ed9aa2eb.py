import argparse

from flaskr import create_app, db

parser = argparse.ArgumentParser()
parser.add_argument("--init", help="Initialize database before running",
                    action="store_true")
args = parser.parse_args()

app = create_app()

if args.init:
    with app.app_context():
        db.init_db()
        print("Initialized database")

app.run("127.0.0.1", 5000, debug=True)