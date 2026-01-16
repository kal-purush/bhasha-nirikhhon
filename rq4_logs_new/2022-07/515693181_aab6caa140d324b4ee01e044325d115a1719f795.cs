namespace Garant.Platform.Models.Configurator.Output
{
    /// <summary>
    /// Класс сконвертированных полей InvestPrice и InvestInclude в массив.
    /// </summary>
    public class ConvertInvestPriceIncludeOutput
    {
        /// <summary>
        /// Название.
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Цена.
        /// </summary>
        public double Price { get; set; }
    }
}