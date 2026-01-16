from umachine import UART
import utime

RESPONSE_TIME = 0.1
START_BYTE = 0xfc
EXTRA_HEADER = [0x01, 0x30]
PT_CONNECT_REQUEST = 0x5a
PT_GET_REQUEST = 0x42
PT_GET_RESPONSE = 0x62
PT_SET_REQUEST = 0x41
PT_SET_RESPONSE = 0x61


class AcAdapter:
    def __init__(self):
        self.uart = UART(2, baudrate=2400, tx=17, rx=16, parity=0)
        self.uart.init(timeout=1000)
        self.state = {
            "power": 0
        }

    def build_packet(self, type, data):
        packet = [START_BYTE, type] + EXTRA_HEADER
        packet.append(len(data))
        packet += data
        packet.append(0xfc - (sum(packet) & 0xff))
        #print("Sending: " + " ".join(["{:02x}".format(x) for x in packet]))
        return packet

    def build_packet2(self, type, data):
        packet = [START_BYTE, type] + EXTRA_HEADER
        packet.append(len(data))
        packet += data
        packet.append(0xfc - (sum(packet) & 0xff))
        print("Sending: " + " ".join(["{:02x}".format(x) for x in packet]))
        return packet

    def decode_mode(self, raw):
        return {
            0x01: "HEAT",
            0x02: "DRY",
            0x03: "COOL",
            0x07: "FAN",
            0x08: "AUTO"
        }.get(raw, "UNKNOWN (0x{:02x})".format(raw))

    def decode_temp(self, raw):
        value = 31 - (raw & 0x0F)
        value += 0.5 if raw & 0x10 else 0
        return value

    def decode_fan(self, raw):
        return {
            0x00: "AUTO",
            0x01: "QUIET",
            0x02: "1",
            0x03: "2",
            0x05: "3",
            0x06: "4"
        }.get(raw, "UNKNOWN (0x{:02x})".format(raw))

    def decode_vane(self, raw):
        return {
            0x00: "AUTO",
            0x01: "1",
            0x02: "2",
            0x03: "3",
            0x04: "4",
            0x05: "5",
            0x07: "SWING"
        }.get(raw, "UNKNOWN (0x{:02x})".format(raw))

    def decode_dir(self, raw):
        return {
            0x00: "NA",
            0x01: "<<",
            0x02: "<",
            0x03: "|",
            0x04: ">",
            0x05: ">>",
            0x08: "<>",
            0x0C: "SWING"
        }.get(raw, "UNKNOWN (0x{:02x})".format(raw))

    def decode_packet(self, raw):
        if raw[0] != START_BYTE:
            raise ValueError("packet has to start with 0xfc")
        
        packet_type = raw[1]

        if packet_type in [PT_GET_RESPONSE]:
            payload_len = raw[4]
            payload = raw[5:5+payload_len]

            raw_data = {}
            data = {}
            if payload[0] == 0x02:  # Type 2
                raw_data["POWER"] = payload[3]
                data["POWER"] = "ON" if raw_data["POWER"] else "OFF"
                raw_data["MODE"] = payload[4]
                data["MODE"] = self.decode_mode(raw_data["MODE"])
                raw_data["TEMP"] = payload[5]
                data["TEMP"] = self.decode_temp(raw_data["TEMP"])
                raw_data["FAN"] = payload[6]
                data["FAN"] = self.decode_fan(raw_data["FAN"])
                raw_data["VANE"] = payload[7]
                data["VANE"] = self.decode_vane(raw_data["VANE"])
                raw_data["DIR"] = payload[10]
                data["DIR"] = self.decode_dir(raw_data["DIR"])
                print(raw_data)
                print(data)
            return data

    def data_to_text(self, data):
        if data is not None:
            return " ".join(["{:02x}".format(x) for x in data])
        return "<None>"
    
    def hp_connect(self):
        print("Connecting...")
        data = bytearray(self.build_packet(PT_CONNECT_REQUEST, [0xCA, 0x01]))
        print("Sending: " + self.data_to_text(data))
        self.uart.write(data)
        utime.sleep(RESPONSE_TIME)
        data = self.uart.read()
        print("Receiving: " + self.data_to_text(data))
        return data

    def hp_request_data(self):
        print("Getting state...")
        data = bytearray(self.build_packet(PT_GET_REQUEST, [0x02] + [0x00]*15))
        print("Sending: " + self.data_to_text(data))
        self.uart.write(data)
        utime.sleep(RESPONSE_TIME)
        data = self.uart.read()
        print("Receiving: " + self.data_to_text(data))

        if data is not None:
            self.state = self.decode_packet(data)
            return self.state
        else:
            return {}

    def hp_set_state(self, state):
        data = [0x00] * 16
        data[0] = 0x01

        if "power" in state:
            data[1] |= 0x01
            data[3] = state["power"]

        if "mode" in state:
            data[1] |= 0x02
            data[4] = state["mode"]

        if "temp" in state:
            data[1] |= 0x04
            data[5] = state["temp"]

        if "fan" in state:
            data[1] |= 0x08
            data[6] = state["fan"]

        if "vane" in state:
            data[1] |= 0x10
            data[7] = state["vane"]

        if "dir" in state:
            data[1] |= 0x80
            data[10] = state["dir"]

        
        print("Setting state...")
        data = bytearray(self.build_packet(PT_SET_REQUEST, data))
        print("Sending: " + self.data_to_text(data))
        self.uart.write(data)
        utime.sleep(RESPONSE_TIME)
        data = self.uart.read()
        print("Receiving: " + self.data_to_text(data))


    def hp_set_power(self, value):
        self.uart.write(bytearray(self.build_packet(PT_SET_REQUEST, [0x01, 0x01, 0x00, value] + [0x00]*12)))

        utime.sleep(RESPONSE_TIME)
        raw = self.uart.read()
        print(raw)

    def hp_set_mode(self, value):
        self.uart.write(bytearray(self.build_packet(PT_SET_REQUEST, [0x01, 0x02, 0x00, 0x00, value] + [0x00]*11)))

        utime.sleep(RESPONSE_TIME)
        raw = self.uart.read()
        print(raw)