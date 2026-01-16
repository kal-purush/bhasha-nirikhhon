using Sol_Demo.Cores;
using Sol_Demo.Models;
using Sol_Demo.Strategies;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Sol_Demo.Contexts
{
    public sealed class SurchargeCalculationContext
    {
        private readonly ConcurrentDictionary<Predicate<ElectricityUnitModel>, IUnitSurchargeCalculation> keyValuePairsUnit
            = new ConcurrentDictionary<Predicate<ElectricityUnitModel>, IUnitSurchargeCalculation>();

        public SurchargeCalculationContext() => this.SetUnitRulesAsync();

        private Task SetUnitRulesAsync()
        {
            Task.Run(() =>
            {
                keyValuePairsUnit.TryAdd((electricityUnitModel) => electricityUnitModel.Unit <= 199, new Upto199UnitStrategy());
                keyValuePairsUnit.TryAdd((electricityUnitModel) => electricityUnitModel.Unit >= 200 && electricityUnitModel.Unit < 400, new Unit200To400UnitStrategy());
                keyValuePairsUnit.TryAdd((electricityUnitModel) => electricityUnitModel.Unit >= 400, new Above400UnitStrategy());
            });

            return Task.CompletedTask;
        }

        public async Task<ElectricitySurchargeModel> GetSurchargeCalculationAsync(ElectricityUnitModel electricityUnitModel)
        {
            return
                await
                (
                   keyValuePairsUnit.FirstOrDefault((keyValuePair) => keyValuePair.Key.Invoke(electricityUnitModel))
                ).Value.SurchargeCalculationAsync(electricityUnitModel);
        }
    }
}