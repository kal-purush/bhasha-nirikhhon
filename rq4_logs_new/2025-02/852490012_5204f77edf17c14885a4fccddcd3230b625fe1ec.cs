using FactorSADEfficiencyOptimizer.Models;
using FactorySADEfficiencyOptimizer.Data;
using FactorySADEfficiencyOptimizer.Models;
using FactorySADEfficiencyOptimizer.ViewModel;
using SupplyChainTesting.MockClasses;

internal static class SimulatorTestsHelpers
{

	public static ProductionTarget MakeProductionTargetBox(int dateDue, int targetQuantity)
	{
		return new ProductionTarget()
		{
			DueDate = dateDue,
			InitAmount = 0,
			IsTargetEnabled = true,
			Status = 0,
			ProductTarget = new Product()
			{
				Quantity = 0,
				ProductName = "box"
			},
			TargetQuantity = targetQuantity
		};
	}
	public static SupplyChainViewModel setupTest()
	{
		IInitializedDataProvider data = new DataProvider_FAKE_Version3();
		SupplyChainViewModel model = new SupplyChainViewModel(data);
		model.Load();
		return model;
	}
}