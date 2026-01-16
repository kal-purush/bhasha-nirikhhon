using DataGeneration.Common;
using DataGeneration.Soap.Maint;
using System;
using System.IO;

namespace DataGeneration.Maint
{
    public class MaintenanceClient : IDisposable
    {
        private readonly EntityMaintenanceSoapClient _client;

        public MaintenanceClient(EndpointSettings endpointSettings)
        {
            _client = new EntityMaintenanceSoapClient(
                endpointSettings.GetBinding(),
                endpointSettings.GetMaintenanceEndpointAddress()
            );
        }

        public static MaintenanceClient LoginLogoutClient(ApiConnectionConfig apiConnectionConfig)
        {
            if (apiConnectionConfig == null)
                throw new ArgumentNullException(nameof(apiConnectionConfig));

            var client = LogoutClient(apiConnectionConfig.EndpointSettings);
            client.Login(apiConnectionConfig.LoginInfo);
            return client;
        }
        public static MaintenanceClient LogoutClient(EndpointSettings endpointSettings)
        {
            if (endpointSettings == null)
                throw new ArgumentNullException(nameof(endpointSettings));

            return new MaintenanceClientDisposable(endpointSettings);
        }
        public void Dispose()
        {
            _client.Close();
        }
        public void GetAndSaveSchema(string version, string endpoint, string path)
        {
            File.WriteAllText(path, GetSchema(version, endpoint));
        }
        public string GetSchema(string version, string endpoint)
        {
            return _client.GetSchema(version, endpoint);
        }
        public void Login(LoginInfo loginInfo)
        {
            _client.Login(loginInfo.Username, loginInfo.Password, loginInfo.Company, loginInfo.Branch, loginInfo.Locale);
        }

        public void Logout()
        {
            _client.Logout();
        }

        public void PutFileSchema(string path)
        {
            PutSchema(File.ReadAllText(path));
        }
        public void PutSchema(string content)
        {
            _client.PutSchema(content);
        }

        private class MaintenanceClientDisposable : MaintenanceClient, IDisposable
        {
            public MaintenanceClientDisposable(EndpointSettings endpointSettings) : base(endpointSettings)
            {
            }

            void IDisposable.Dispose()
            {
                Logout();
                base.Dispose();
            }
        }
    }
}