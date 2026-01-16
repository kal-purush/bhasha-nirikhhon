import pytest
from app import create_app
from app import db
from flask.signals import request_finished
from app.models.Planet import Planet


@pytest.fixture
def app():
    app = create_app({"TESTING": True})

    @request_finished.connect_via(app)
    def expire_session(sender, response, **extra):
        db.session.remove()

    with app.app_context():
        db.create_all()
        yield app

    with app.app_context():
        db.drop_all()


@pytest.fixture
def client(app):
    return app.test_client()

@pytest.fixture
def two_saved_planets(app):
    # Arrange
    chi_planet = Planet(name="Chiland",
                      description="The coolest place ever",
                      color="lavender and blue")
    bahareh_planet = Planet(name="Baharehland",
                         description="No, the coolest place ever. More than Chiland", 
                         color="rainbow awesomeness")

    db.session.add_all([chi_planet, bahareh_planet])
    db.session.commit()