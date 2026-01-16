from core_rover.server.subsystems.fieldbus import InvalidControllerParametersError
from core_rover.server.subsystems.drivers.base import BaseDriver


class BLDCMotor(BaseDriver):
    def echo(self):
        response_dict = self._request(respond=True, command=0)
        return response_dict

    def start(self):
        response_dict = self._request(respond=True, command='\x01')
        return response_dict

    def stop(self):
        response_dict = self._request(respond=True, command='\x02')
        return response_dict

    def set_power(self, power):
        response_dict = self._request(respond=True, command='\x04', params=[power])
        return response_dict

    def set_direction(self, direction):
        if direction in [-1, 1]:
            response_dict = self._request(respond=True, command='\x08', params=[direction])
            return response_dict
        else:
            error_message = 'Invalid direction value for BLDC motor controller'
            raise InvalidControllerParametersError(message=error_message)
