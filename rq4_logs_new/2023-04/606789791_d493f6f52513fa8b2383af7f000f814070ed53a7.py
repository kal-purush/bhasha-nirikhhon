from umachine import UART
import utime
import uasyncio

RESPONSE_TIME = 0.1
START_BYTE = 0xfc
EXTRA_HEADER = [0x01, 0x30]
PT_CONNECT_REQUEST = 0x5a
PT_GET_REQUEST = 0x42
PT_GET_RESPONSE = 0x62
PT_SET_REQUEST = 0x41
PT_SET_RESPONSE = 0x61

class AcRequest:
    def __init__(self):
        self.ptype = 0x00
        self.payload_length = 0
        self.payload = []

    def build(self):
        self.payload_length = len(self.payload)
        self.raw = [START_BYTE, self.ptype] + EXTRA_HEADER
        self.raw += [self.payload_length]
        self.raw += self.payload
        self.raw += [0xfc - (sum(self.raw) & 0xff)]
        return bytearray(self.raw)

    def __str__(self) -> str:
        packet = self.build()
        return " ".join(["{:02x}".format(x) for x in packet])

class AcResponse:
    def __init__(self, raw):
        self.raw = raw

        self.ptype = self.raw[1]
        self.payload_length = self.raw[4]
        self.payload = self.raw[5:5+self.payload_length]

        self.data = {}
        if self.ptype == PT_GET_RESPONSE:
            if self.payload[0] == 0x02:
                self.data["POWER"] = (self.payload[3], "ON" if self.payload[3] else "OFF")
                self.data["MODE"] = (self.payload[4], self.decode_mode(self.payload[4]))
                self.data["TEMP"] = (self.payload[5], self.decode_temp(self.payload[5]))
                self.data["FAN"] = (self.payload[6], self.decode_fan(self.payload[6]))
                self.data["VANE"] = (self.payload[7], self.decode_vane(self.payload[7]))
                self.data["DIR"] = (self.payload[10], self.decode_dir(self.payload[10]))
            if self.payload[0] == 0x03:
                self.data["ROOMTEMP"] = (self.payload[3], self.decode_roomtemp(self.payload[3]))        

    def decode_mode(self, value):
        return {
            0x01: "HEAT",
            0x02: "DRY",
            0x03: "COOL",
            0x07: "FAN",
            0x08: "AUTO"
        }.get(value, "UNKNOWN (0x{:02x})".format(value))

    def decode_temp(self, value):
        temp = 31 - (value & 0x0F)
        temp += 0.5 if value & 0x10 else 0
        return temp

    def decode_roomtemp(self, value):
        temp = 10 + value
        return temp

    def decode_fan(self, value):
        return {
            0x00: "AUTO",
            0x01: "QUIET",
            0x02: "1",
            0x03: "2",
            0x05: "3",
            0x06: "4"
        }.get(value, "UNKNOWN (0x{:02x})".format(value))

    def decode_vane(self, value):
        return {
            0x00: "AUTO",
            0x01: "1",
            0x02: "2",
            0x03: "3",
            0x04: "4",
            0x05: "5",
            0x07: "SWING"
        }.get(value, "UNKNOWN (0x{:02x})".format(value))

    def decode_dir(self, value):
        return {
            0x00: "NA",
            0x01: "<<",
            0x02: "<",
            0x03: "|",
            0x04: ">",
            0x05: ">>",
            0x08: "<>",
            0x0C: "SWING"
        }.get(value, "UNKNOWN (0x{:02x})".format(value))

    def __str__(self) -> str:
        return " ".join(["{:02x}".format(x) for x in self.raw])

class AcAdapter:
    def __init__(self):
        self.uart = UART(2, baudrate=2400, tx=17, rx=16, parity=0)
        self.uart.init(timeout=50)
        self.swriter = uasyncio.StreamWriter(self.uart, {})
        self.sreader = uasyncio.StreamReader(self.uart)
        self.connected = False
        self.waiting_for_response = False

    def _create_connect_packet(self):
        packet = AcRequest()
        packet.ptype = PT_CONNECT_REQUEST
        packet.payload = [0xCA, 0x01]
        return packet
    
    def _create_get_state_packet1(self):
        packet = AcRequest()
        packet.ptype = PT_GET_REQUEST
        packet.payload = [0x02] + [0x00]*15
        return packet
    
    def _create_get_state_packet2(self):
        packet = AcRequest()
        packet.ptype = PT_GET_REQUEST
        packet.payload = [0x03] + [0x00]*15
        return packet
    
    def _create_set_state_packet(self, state):
        packet = AcRequest()
        packet.ptype = PT_SET_REQUEST
        packet.payload = [0x01] + [0x00]*15

        if "POWER" in state:
            packet.payload[1] |= 0x01
            packet.payload[3] = state["POWER"]

        if "MODE" in state:
            packet.payload[1] |= 0x02
            packet.payload[4] = state["MODE"]

        if "TEMP" in state:
            t = int(state["TEMP"]*2)
            value = 0x10 if t%2 == 1 else 0x00
            value |= (31-t//2)&0x0F 
            packet.payload[1] |= 0x04
            packet.payload[5] = value

        if "FAN" in state:
            packet.payload[1] |= 0x08
            packet.payload[6] = state["FAN"]

        if "VANE" in state:
            packet.payload[1] |= 0x10
            packet.payload[7] = state["VANE"]

        if "DIR" in state:
            packet.payload[1] |= 0x80
            packet.payload[10] = state["DIR"]

        return packet

    def bytearray_to_text(self, raw):
        if raw is not None:
            return " ".join(["{:02x}".format(x) for x in raw])
        return "<None>"

    async def _send_packet_and_wait_for_response(self, packet: AcRequest) -> AcResponse:        
        while self.waiting_for_response:
            await uasyncio.sleep(1)
            print("waiting...")
        print("Sending:   " + str(packet))
        self.waiting_for_response = True
        self.swriter.write(packet.build())
        await self.swriter.drain()
        #utime.sleep(RESPONSE_TIME)
        res = await self.sreader.read(-1)
        self.waiting_for_response = False
        if res is not None:
            response = AcResponse(res)
            print("Receiving: " + str(response))
            return response
        print("Receiving: <None>")
        return None

    async def ac_connect(self) -> AcResponse:
        print("Connecting to AC...")
        packet = self._create_connect_packet()
        self.connected = True
        return await self._send_packet_and_wait_for_response(packet)

    async def ac_get_state1(self) -> AcResponse:
        print("Reading AC state 1...")
        packet = self._create_get_state_packet1()
        return await self._send_packet_and_wait_for_response(packet)

    async def ac_get_state2(self) -> AcResponse:
        print("Reading AC state 2...")
        packet = self._create_get_state_packet2()
        return await self._send_packet_and_wait_for_response(packet)

    async def ac_set_state(self, state: dict) -> AcResponse:
        print("Writing AC state...")
        packet = self._create_set_state_packet(state)
        return await self._send_packet_and_wait_for_response(packet)