from Sources.ESP.network import network


class ESPServer:
    observers = []

    def __init__(self):
        pass
        # connection = Socket.connection(onSocketData)

    def on_socket_data(self, bytes):
        for observer in self.observers:
            observer.onReceivedData(bytes)


class ESPClient:
    home_wifi_SSID = None
    home_wifi_password = None

    internal_network = network.WLAN(network.STA_IF)

    def __init__(self, home_wifi_ssid, home_wifi_password):
        server = ESPServer()
        server.observers.append(self)

        self.home_wifi_SSID = home_wifi_ssid
        self.home_wifi_password = home_wifi_password

    def connect_to_home_wifi(self):
        self.internal_network.active(True)
        self.internal_network.connect(self.home_wifi_SSID, self.home_wifi_password)

    def isconnected(self):
        return self.internal_network.isconnected()

    def ip_address(self) -> str:
        return self.internal_network.ifconfig()[0]

    def scan(self):
        return self.internal_network.scan()

    def onReceivedData(self, bytes):
        line = bytes


client = ESPClient("", "")

print(client.scan()[0])