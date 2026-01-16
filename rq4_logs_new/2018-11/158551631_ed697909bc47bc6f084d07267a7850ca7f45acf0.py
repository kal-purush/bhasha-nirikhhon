import responder
from marshmallow import ValidationError

from bierepong.api.schemas import sensor_schema, sensors_schema
from bierepong.controllers import SensorControler
from bierepong.models import Sensor

api = responder.API()
sensor_controller = SensorControler()


@api.route("/sensor/")
class SensorCollection:
    """Handle Sensor list and create"""

    async def on_get(self, req, resp):
        resp.media = sensors_schema.dump(sensor_controller.list())

    async def on_post(self, req, resp):
        data = await req.media()
        try:
            data = sensor_schema.load(data)
        except ValidationError as err:
            print(err)
            resp.status_code = api.status_codes.HTTP_400
            return

        sensor = Sensor(status=data['status'])
        sensor_controller.save(sensor)

        resp.media = sensor_schema.dump(sensor)

    def on_request(self, req, resp):
        resp.status_code = api.status_codes.HTTP_405


@api.route("/sensor/{sensor_id}/")
class SensorResource:
    """Handle Sensor get / update / delete"""

    async def on_get(self, req, resp, *, sensor_id):
        sensor = self._get_sensor(resp=resp, sensor_id=sensor_id)
        if sensor is None:
            return
        resp.media = sensor_schema.dump(sensor)

    async def on_delete(self, req, resp, *, sensor_id):
        sensor = self._get_sensor(resp=resp, sensor_id=sensor_id)
        if sensor is None:
            return
        sensor_controller.delete_obj(sensor)
        resp.status_code = api.status_codes.HTTP_202

    async def on_put(self, req, resp, *, sensor_id):
        data = await req.media()
        try:
            data = sensor_schema.load(data)
        except ValidationError:
            resp.status_code = api.status_codes.HTTP_400
            return

        sensor = self._get_sensor(resp=resp, sensor_id=sensor_id)
        if sensor is None:
            return

        sensor.status = data['status']
        sensor_controller.save(sensor)
        resp.media = sensor_schema.dump(sensor)

    async def on_request(self, req, resp, *, sensor_id):
        resp.status_code = api.status_codes.HTTP_405

    @staticmethod
    def _get_sensor(resp, sensor_id):
        sensor = sensor_controller.get(obj_id=sensor_id)
        if sensor is None:
            resp.status_code = api.status_codes.HTTP_404
        return sensor


if __name__ == '__main__':
    api.run()