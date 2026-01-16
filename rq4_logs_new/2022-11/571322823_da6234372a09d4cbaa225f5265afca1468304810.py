from .WindowClient import WindowClient
from .ui import UiWindow, MenuValue
import ipaddress
import sys
import snimpy

class CdpClient(WindowClient):
    CdpCapabilities = {
            0b1000000: "Repeater",
            0b100000: "IGMP Capable",
            0b10000: "Host",
            0b1000: "Switch",
            0b100: "Source Route Bridge",
            0b10: "Transparent Bridge",
            0b1: "Router"
            }

    def parseCapabilities(self, val):
        retval = {}
        retval['decoded'] = []
        for cap, capVal in self.CdpCapabilities.items():
            if val & cap:
                retval['decoded'].append(capVal)
        remainder = "{:025b}".format(val >> 7)
        retval['remainder'] = remainder
        return retval

    def getCdpCacheTable(self):
        results = {}
        m = self.M
        for i in m.cdpCacheAddress:
            result = {
                    'ifDescr': self._doTryGet("ifDescr", i[0]),
                    'cdpCacheAddress': self._doTryGet("cdpCacheAddress", i),
                    'cdpCacheVersion': self._doTryGet("cdpCacheVersion", i),
                    'cdpCacheDeviceId': self._doTryGet("cdpCacheDeviceId", i),
                    'cdpCacheDevicePort': self._doTryGet("cdpCacheDevicePort", i),
                    'cdpCachePlatform': self._doTryGet("cdpCachePlatform", i),
                    'cdpCacheCapabilities': self.parseCapabilities(int.from_bytes(self._doTryGet("cdpCacheCapabilities", i), "big")),
                    'cdpCacheVTPMgmtDomain': self._doTryGet("cdpCacheVTPMgmtDomain", i),
                    'cdpCacheNativeVLAN': self._doTryGet("cdpCacheNativeVLAN", i),
                    'cdpCacheDuplex': self._doTryGet("cdpCacheDuplex", i),
                    'cdpCacheApplianceID': self._doTryGet("cdpCacheApplianceID", i),
                    'cdpCacheVlanID': self._doTryGet("cdpCacheVlanID", i),
                    'cdpCachePowerConsumption': self._doTryGet("cdpCachePowerConsumption", i),
                    'cdpCacheMTU': self._doTryGet("cdpCacheMTU", i),
                    'cdpCacheSysName': self._doTryGet("cdpCacheSysName", i),
                    'cdpCacheSysObjectID': self._doTryGet("cdpCacheSysObjectID", i),
                    'cdpCachePrimaryMgmtAddrType': self._doTryGet("cdpCachePrimaryMgmtAddrType", i),
                    'cdpCachePrimaryMgmtAddr': self._doTryGet("cdpCachePrimaryMgmtAddr", i),
                    'cdpCacheSecondaryMgmtAddrType': self._doTryGet("cdpCacheSecondaryMgmtAddrType", i),
                    'cdpCacheSecondaryMgmtAddr': self._doTryGet("cdpCacheSecondaryMgmtAddr", i),
                    'cdpCachePhysLocation': self._doTryGet("cdpCachePhysLocation", i),
                    'cdpCacheLastChange': self._doTryGet("cdpCacheLastChange", i)
                    }
            try:
                ipAddress = ipaddress.ip_address(result['cdpCacheAddress'])
                result['cdpCacheAddress'] = ipAddress
            except Exception as e:
                print(f"ipAddress Error: {e}")
                pass
            if result['ifDescr'] not in results.keys():
                results[result['ifDescr']] = {}
            results[result['ifDescr']][i[1]] = result
        return results

    def getCieIfStatusListTable(self, m, i):
        results = {}
        operMode = m.cieInterfacesOperMode[1]
        results['cieInterfacesOperMode'] = []
        try:
            for k, v in operMode.iteritems():
                results['cieInterfacesOperMode'].append((k, v))
        except:
            pass
        results['cieInterfacesOperCause'] = m.cieInterfacesOperCause[i]
        results['cieInterfaceOwnershipBitmap'] = m.cieInterfaceOwnershipBitmap[i]
        return results

    def __init__(
            self,
            host,
            version,
            community=None,
            userName=None,
            authPass=None,
            privPass=None,
            authProtocol="SHA",
            privProtocol="AES",
            port=161,
            extraMibs=[]):
        super().__init__(
            host,
            version,
            community,
            userName,
            authPass,
            privPass,
            authProtocol,
            privProtocol,
            port,
            extraMibs)

class CdpNeighborsUI(UiWindow):
    clientType = CdpClient
    #clients = super().clients
    #snmp_data = super().snmp_data

    def __exit(self):
        sys.exit(0)

    def _onClientChanged(self):
        uxCdpInterfaces = self._ui.get('uxCdpInterfaces')
        uxCdpInterfaces.clear()
        selectedClient = self._ui.get('uxClients').get()
        for key, val in self.snmp_data[selectedClient].items():
            thisVal = MenuValue(key, val)
            uxCdpInterfaces.add_item(thisVal)
        self._onCdpInterfaceChanged()

    def _onCdpInterfaceChanged(self):
        uxCdpDetail = self._ui.get('uxCdpDetail')
        selectedInterface = self._ui.get('uxCdpInterfaces').get().get()
        uxCdpDetail.clear()
        for sKey, sVal in selectedInterface.items():
            for key, val in sVal.items():
                thisVal = ""
                if (type(val) == type(None)):
                    continue
                if (
                        (type(val) == str)
                        or (type(val) == snimpy.basictypes.String)
                        or (type(val) == ipaddress.IPv4Address)
                        or (type(val) == snimpy.basictypes.Integer)
                        or (type(val) == snimpy.basictypes.Enum)
                        or (type(val) == snimpy.basictypes.Unsigned32)
                        ):
                    firstLine = True
                    for line in str(val).splitlines():
                        if firstLine == True:
                            firstLine = False
                            thisVal = f"{key}: {str(line)}"
                            uxCdpDetail.add_item(thisVal)
                        else:
                            thisVal = f"    {str(line)}"
                            uxCdpDetail.add_item(thisVal)
                elif (type(val) == dict):
                    lines = self._printDict(val)
                    firstLine = True
                    for line in lines:
                        if firstLine == True:
                            firstLine = False
                            thisVal = f"{key}: {str(line)}"
                            uxCdpDetail.add_item(thisVal)
                        else:
                            thisVal = f"    {str(line)}"
                            uxCdpDetail.add_item(thisVal)
                else:
                    thisVal = f"{key}: {type(val)}"
                    uxCdpDetail.add_item(thisVal)
    def getSNMP(self):
        uxClients = self._ui.get("uxClients")
        uxClients.clear()
        for client, m in self.clients.items():
            self.snmp_data[client] = m.getCdpCacheTable()
            uxClients.add_item(client)
        self._onClientChanged()
        return self.snmp_data

    def setSNMP(self, host, version, userName, authPass, privPass, extraMibs):
        self.clients[host] = super()._setSNMP(self.clientType, host, version, userName, authPass, privPass, extraMibs)

    def run(self):
        self.getSNMP()

    def __init__(self, thisUI):
        self._ui = thisUI
        self._ui.update_grid_height_width(height=4, width=3)
        self._ui.add("uxExitLabel", self._ui.root.add_button('Exit', 3, 0, command=self.__exit))

        uxClients = self._ui.root.add_scroll_menu('Clients', 0, 0)
        uxClients.set_on_selection_change_event(self._onClientChanged)
        self._ui.add("uxClients", uxClients)

        uxCdpInterfaces = self._ui.root.add_scroll_menu('CDP Neighbors', 0, 1)
        uxCdpInterfaces.set_on_selection_change_event(self._onCdpInterfaceChanged)
        self._ui.add("uxCdpInterfaces", uxCdpInterfaces)

        uxCdpDetail = self._ui.root.add_scroll_menu('Neighbor', 1, 1, column_span=2, row_span=2)
        self._ui.add("uxCdpDetail", uxCdpDetail)