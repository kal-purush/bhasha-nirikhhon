using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.ComponentModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using WPFTesting.Data;
using WPFTesting.Models;
using WPFTesting.Shapes;
using WPFTesting.ViewModel;

namespace FactorSADEfficiencyOptimizer.ViewModel;
public class AnalizorModel
{
	public int Duration { get; set; }
	public event PropertyChangedEventHandler? PropertyChanged;
	private ObservableCollection<EndpointUIValues> _endpointList = new ObservableCollection<EndpointUIValues>();
	public ObservableCollection<EndpointUIValues> EndpointList
	{
		get => _endpointList;
		set
		{
			_endpointList = value;
			OnPropertyChanged(nameof(EndpointList));
		}
	}

	private List<Shipment> _shipmentList = new List<Shipment>();
	public List<Shipment> ShipmentList
	{
		get { return _shipmentList; }
		set
		{
			_shipmentList = value;
			OnPropertyChanged(nameof(ShipmentList));
		}
	}

	public string ShortestPath;


	private ObservableCollection<SupplierUIValues> _supplierList = new ObservableCollection<SupplierUIValues>();
	public ObservableCollection<SupplierUIValues> SupplierList
	{
		get => _supplierList;
		set
		{
			_supplierList = value;
			OnPropertyChanged(nameof(SupplierList));
		}
	}
	public AnalizorModel(SupplyChainViewModel model)
	{
		ShortestPath = "";
		foreach (var supplier in model.SupplierList)
		{
			SupplierUIValues sup = new SupplierUIValues();
			sup.supplier = new Supplier();
			sup.supplier.Name = supplier.supplier.Name;
			sup.supplier.ComponentInventory = supplier.supplier.ComponentInventory;
			sup.supplier.ProductInventory = supplier.supplier.ProductInventory;
			sup.Position = new System.Drawing.Point();
			AddSupplier(sup);
		}
	}

	protected void OnPropertyChanged(string? name = null)
	{
		if (name is not null)
		{
			PropertyChanged?.Invoke(this, new PropertyChangedEventArgs(name));
		}
	}

	public void AddEndpoint(EndpointUIValues endpoint)
	{
		EndpointList.Add(endpoint);
		OnPropertyChanged(nameof(EndpointList));
	}

	public void AddSupplier(SupplierUIValues supplier)
	{
		SupplierList.Add(supplier);
		OnPropertyChanged(nameof(SupplierList));
	}

	public void AdvanceTime()
	{
		foreach (Shipment delivery in ShipmentList)
		{
			//shipOrder
			var sentProds = delivery.Sender.ShipOrder(delivery.Products);
			List<Product> listSent = new List<Product>();
			foreach (var prod in sentProds)
			{
				listSent.Add(prod);
			}
			//Receive 
			delivery.Receiver.Receive(listSent);
		}
		foreach (var v in SupplierList)
		{
			v.supplier.Process();
		}
		foreach (var e in EndpointList)
		{
			var end = (EndpointNode)e.supplier;
			end.ProduceProduct();
		}
	}

	public Shipment ShipmentFirstSuplier(Supplier target)
	{
		Shipment targetShipment = new Shipment();
		targetShipment.Sender = target;
		targetShipment.Products.Clear();
		Product toShip = target.ProductInventory.First();
		targetShipment.Products.Add(new Product());
		targetShipment.Products[0].ProductName = toShip.ProductName;
		targetShipment.Products[0].Quantity = 5;
		return targetShipment;
	}

	public void PassTimeUntilDuration()
	{
		for (int i = 0; i < Duration; i++)
		{
			AdvanceTime();
		}
	}
}