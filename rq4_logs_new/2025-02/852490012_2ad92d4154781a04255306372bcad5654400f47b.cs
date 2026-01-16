using FactorSADEfficiencyOptimizer.Models;
using FactorySADEfficiencyOptimizer.Shapes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FactorySADEfficiencyOptimizer.Models.AnalyzerTrackers;

public class Snapshot
{
	public List<EndpointUIValues> Endpoints { get; set; }
	public List<Shipment> Shipments { get; set; }
	public List<SupplierUIValues> Suppliers { get; set; }
	public List<ProductionTarget> Targets { get; set; }
}