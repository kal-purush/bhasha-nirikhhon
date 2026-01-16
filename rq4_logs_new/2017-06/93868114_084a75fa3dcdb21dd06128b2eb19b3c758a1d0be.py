from webcalc import app, mongo


with app.app_context():

    mongo.db.operations.insert(
        dict(
            name="+",
            pattern="{{ a + b }}"
        )
    )

    mongo.db.operations.insert(
        dict(
            name="x",
            pattern="{{ a * b }}"
        )
    )
