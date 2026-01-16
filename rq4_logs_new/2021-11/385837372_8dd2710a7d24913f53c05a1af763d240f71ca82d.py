
from http import HTTPStatus

from sqlalchemy.orm import sessionmaker

from src.models.MeasurementModel import MeasurementModel, MeasurementSchema
from src.services.core.db.engine import get_engine


def measurement_list_get():
    session = sessionmaker(bind=get_engine())()
    measurements = session.query(MeasurementModel).all()

    return MeasurementSchema(many=True).dump(measurements), HTTPStatus.OK