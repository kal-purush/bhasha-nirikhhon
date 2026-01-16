using GateKeeper;
using GateKeeper.Configuration;
using GateKeeper.Cryptogrophy;
using GateKeeper.Exceptions;
using Microsoft.Extensions.Configuration;

namespace BudgetTracker.BudgetSquirrel.WebApi.Tests.Utils
{
    public class EncryptionHelper
    {
        private GateKeeperConfig _gateKeeperConfig;
        private ICryptor _cryptor;

        public EncryptionHelper(IConfiguration appConfig)
        {
            _gateKeeperConfig = ConfigurationReader.FromAppConfiguration(appConfig);
            _cryptor = new Rfc2898Encryptor();
        }

        public string Decrypt(string val)
        {
            return _cryptor.Decrypt(val, _gateKeeperConfig.EncryptionKey, _gateKeeperConfig.Salt);
        }
    }
}