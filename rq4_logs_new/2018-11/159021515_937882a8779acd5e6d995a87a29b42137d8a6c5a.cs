using DS.Dtos.Routes;
using System;

namespace DS.Dtos.Builders
{
    public class CreateDeliveryRouteDtoBuilder : CreateDeliveryRouteDto
    {
        public CreateDeliveryRouteDtoBuilder WithAllValidFields()
        {
            Start = StringGenerator.Random(1);
            End = StringGenerator.Random(1);
            Cost = IntGenerator.RandomCost();

            return this;
        }

        public CreateDeliveryRouteDtoBuilder With(Action<CreateDeliveryRouteDtoBuilder> action)
        {
            action(this);

            return this;
        }
    }
}