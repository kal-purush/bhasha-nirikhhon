using DS.Contracts.DataAccess.Repositories;
using DS.Contracts.OperationResponse;
using DS.DomainModel.Entities;
using DS.DomainModel.Factories;
using DS.Dtos.Exceptions;
using DS.Dtos.ResponseMessages;
using DS.Dtos.Routes;
using DS.Handlers.Abstract;
using DS.Handlers.Requests;
using DS.Handlers.Responses;
using DS.Handlers.Strategies.Factories;
using FluentValidation;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DS.Handlers
{
    public class GetTheCheapestDeliveryCostHandler : HandlerBase<GetTheCheapestDeliveryCostHandlerRequest, GetTheCheapestDeliveryCostHandlerResponse>
    {
        private int? _theCheapestDeliveryCost;

        private readonly IDeliveryRouteReadOnlyRepository _deliveryRouteReadOnlyRepository;
        private readonly IRouteFactory _routeFactory;

        public GetTheCheapestDeliveryCostHandler(
            IDeliveryRouteReadOnlyRepository deliveryRouteReadOnlyRepository,
            IRouteFactory routeFactory,
            ILogger<GetTheCheapestDeliveryCostHandler> logger,
            IValidator<GetTheCheapestDeliveryCostHandlerRequest> validator,
            IHandlerExceptionStrategyFactory handlerExceptionStrategyFactory)
            : base(logger, validator, handlerExceptionStrategyFactory)
        {
            _deliveryRouteReadOnlyRepository = deliveryRouteReadOnlyRepository;
            _routeFactory = routeFactory;
        }

        protected override async Task<IOperationResponse<GetTheCheapestDeliveryCostHandlerResponse>> HandleAsync(GetTheCheapestDeliveryCostHandlerRequest request)
        {
            var deliveryRoutes = await _deliveryRouteReadOnlyRepository.GetAllAsync();
            if (deliveryRoutes == null || !deliveryRoutes.Any())
                throw new DataAccessNotFoundException(ResponseMessages.Route.DoesNotExist);

            var routes = BuildRoute(request.Route);
            var theCheapestDeliveryCost = CalculateTheCheapestDeliveryCost(routes, deliveryRoutes);

            return Success(new GetTheCheapestDeliveryCostHandlerResponse(theCheapestDeliveryCost));
        }

        #region Private Methods

        private Route BuildRoute(string route)
        {
            var routes = route.Split(new[] { '-' }, StringSplitOptions.RemoveEmptyEntries);
            var dto = new CreateRouteDto
            {
                Start = routes[0].ToUpper(),
                End = routes[1].ToUpper()
            };

            return _routeFactory.Create(dto);
        }

        private int? CalculateTheCheapestDeliveryCost(Route route, IEnumerable<DeliveryRoute> deliveryRoutes)
        {
            var path = string.Empty;
            var visited = new Dictionary<string, int>();

            var deliveryRoutesOfStart = deliveryRoutes.Where(deliveryRoute => deliveryRoute.Start.Equals(route.Start, StringComparison.OrdinalIgnoreCase));
            foreach (var deliveryRoute in deliveryRoutesOfStart)
            {
                FindTheCheapestDeliveryCost
                (
                    coveredRoute: path,
                    visited: ref visited,
                    route: deliveryRoute,
                    end: route.End,
                    aggregateDeliveryCost: deliveryRoute.Cost,
                    currentCost: 0,
                    allDeliveryRoutes: deliveryRoutes
                );
            }

            return _theCheapestDeliveryCost;
        }

        private void FindTheCheapestDeliveryCost(string coveredRoute, ref Dictionary<string, int> visited, DeliveryRoute route, string end, int aggregateDeliveryCost, int currentCost, IEnumerable<DeliveryRoute> allDeliveryRoutes)
        {
            aggregateDeliveryCost = UpdateAggregateDeliveryCost(aggregateDeliveryCost, currentCost);

            if (!string.IsNullOrWhiteSpace(coveredRoute))
            {
                UpdateVisited(coveredRoute, ref visited);

                if (IsRepeated(coveredRoute, visited))
                    return;
            }

            if (route.End.Equals(end, StringComparison.OrdinalIgnoreCase))
                UpdateTheCheapestDeliveryCost(aggregateDeliveryCost);

            var deliveryRoutes = allDeliveryRoutes.Where(deliveryRoute => deliveryRoute.Start.Equals(route.End, StringComparison.OrdinalIgnoreCase));
            foreach (var deliveryRoute in deliveryRoutes)
            {
                coveredRoute = UpdateCoveredRoute(route.End, deliveryRoute);

                FindTheCheapestDeliveryCost
                (
                    coveredRoute: coveredRoute,
                    visited: ref visited,
                    route: deliveryRoute,
                    end: end,
                    aggregateDeliveryCost: aggregateDeliveryCost,
                    currentCost: deliveryRoute.Cost,
                    allDeliveryRoutes: allDeliveryRoutes
                );

                UnMarkVisited(coveredRoute, visited);
            }
        }

        private int UpdateAggregateDeliveryCost(int aggregateDeliveryCost, int currentCost) => aggregateDeliveryCost + currentCost;

        private void UpdateVisited(string coveredRoute, ref Dictionary<string, int> visited)
        {
            if (visited.TryGetValue(coveredRoute, out var count))
                visited[coveredRoute]++;
            else
                visited.Add(coveredRoute, 1);
        }

        private bool IsRepeated(string coveredRoute, Dictionary<string, int> visited)
        {
            visited.TryGetValue(coveredRoute, out var value);

            return value > 1;
        }

        private void UpdateTheCheapestDeliveryCost(int aggregateDeliveryCost)
        {
            if (aggregateDeliveryCost <= (_theCheapestDeliveryCost ?? int.MaxValue))
                _theCheapestDeliveryCost = aggregateDeliveryCost;
        }

        private string UpdateCoveredRoute(string start, DeliveryRoute deliveryRoute) => start + deliveryRoute.End;

        private void UnMarkVisited(string coveredRoute, Dictionary<string, int> visited)
        {
            visited[coveredRoute]--;

            if (visited[coveredRoute] < 0)
                visited[coveredRoute] = 0;
        }

        #endregion
    }
}