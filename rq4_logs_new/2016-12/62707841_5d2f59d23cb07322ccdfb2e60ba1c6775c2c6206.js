RbDriverCmxNetworkMerakiConfigurationModel.$inject = ['$relayboxApiModelFactory'];
export default function RbDriverCmxNetworkMerakiConfigurationModel($relayboxApiModelFactory) {
  return {
      forNetwork: function (networkShortId) {
          return $relayboxApiModelFactory('networks/' + networkShortId + '/meraki-configuration');
        }
  }
}