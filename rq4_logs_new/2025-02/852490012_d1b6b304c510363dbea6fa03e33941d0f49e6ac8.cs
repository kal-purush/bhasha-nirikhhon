using FactorySADEfficiencyOptimizer.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Input;

namespace FactorySADEfficiencyOptimizer.UIComponent.EventArguments
{
    public class SelectedShipmentWindowMouseButtonEventArgs : MouseButtonEventArgs
    {
        public Shipment selected {  get; private set; }
        public SelectedShipmentWindowMouseButtonEventArgs(MouseDevice mouse, int timestamp, MouseButton button) : base(mouse, timestamp, button)
        {
        }

        public void AssignSelectedShipment(Shipment shipment)
        {
            this.selected = shipment;
        }
    }
}