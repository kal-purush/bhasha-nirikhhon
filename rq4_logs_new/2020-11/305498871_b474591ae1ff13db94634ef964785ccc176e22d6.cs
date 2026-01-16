namespace Task4
{
    class Article
    {
        public Article(string productName, Store storeName, int price)
        {
            ProductName = productName;
            StoreName = storeName.Name;
            Price = price;
        }

        public string ProductName { get; }
        public string StoreName { get; }
        public int Price { get; }
        public override string ToString()
        {

            return $"Product: {ProductName}\n" +
                   $"Store: {StoreName}\n" +
                   $"Price: {Price} UAH";
        }
    }

}