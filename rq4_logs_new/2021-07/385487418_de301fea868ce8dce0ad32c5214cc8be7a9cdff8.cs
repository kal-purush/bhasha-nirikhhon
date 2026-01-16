namespace Exercise5
{
    public abstract class Advert
    {
        protected string _advertName; 
        protected string _advertType;
        protected int _totalCost = 0;
        protected int _materialCost;
        protected int _productionStaffCost;
        protected int _mediaCost;

        protected Advert(string Name, string Type, int MaterialCost, int ProductionStaffCost, int MediaCost)
        {
            _advertName = Name;
            _advertType = Type;
            _materialCost = MaterialCost;
            _productionStaffCost = ProductionStaffCost;
            _mediaCost = MediaCost;
            _totalCost += _materialCost + _productionStaffCost + _mediaCost;
        }
    }
}