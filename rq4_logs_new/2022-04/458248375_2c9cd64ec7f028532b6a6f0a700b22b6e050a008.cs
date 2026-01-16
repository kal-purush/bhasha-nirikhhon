using System.Collections.Generic;

namespace Meetins.Models.KudaGo
{
    /// <summary>
    /// Выходная модель для списка доступных мест из сервиса KudaGo.
    /// </summary>
    public class KudaGoPlacesOutput
    {
        /// <summary>
        /// Количество доступных мест.
        /// </summary>
        public int Count { get; set; }

        /// <summary>
        /// Следующее место.
        /// </summary>
        public string Next { get; set; }

        /// <summary>
        /// Предыдущее место.
        /// </summary>
        public string Previous { get; set; }

        /// <summary>
        /// Навигационное свойство.
        /// </summary>
        public IEnumerable<Results> Results { get; set; }
    }
}