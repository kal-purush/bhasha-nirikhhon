using Core.Domain;

namespace WireMockSpecFlowTests.Domain
{
    public class IntegrationTestDataBuilder
    {
        private string _id = "1234567890";

        private string _name = "Any name";

        private string _description = "Any description";

        private string _source = "Unknown";

        private bool _serviceBoard = false;

        public static IntegrationTestDataBuilder AnIntegration()
        {
            return new IntegrationTestDataBuilder();
        }

        public static IntegrationTestDataBuilder AConnectWiseIntegration()
        {
            return ConnectWiseIntegration;
        }

        public IntegrationTestDataBuilder WithName(string name)
        {
            _name = name;
            return this;
        }

        public IntegrationTestDataBuilder WithServiceBoard()
        {
            _serviceBoard = true;
            return this;
        }


        public Integration Build()
        {
            return new Integration
            {
                Id = _id,
                Name = _name,
                Description = _description,
                Source = _source,
                ServiceBoard = _serviceBoard
            };
        }

        private static IntegrationTestDataBuilder ConnectWiseIntegration
            => new IntegrationTestDataBuilder
            {
                _id = "4242424242424242",
                _name = "ConnectWiseMR",
                _description = "This is a ConnectWise Integration",
                _source = "ConnectWise",
                _serviceBoard = false
            };

    }
}