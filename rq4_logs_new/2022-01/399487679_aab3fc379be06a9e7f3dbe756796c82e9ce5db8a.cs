using System.ComponentModel.DataAnnotations;
using System.Text.Json.Serialization;
using Igtampe.Neco.Common.Income;

namespace Igtampe.Neco.Common.Taxes {

    /// <summary>A tax bracket with a defined tax rate, a range that it applies to, and an income type that it applies to</summary>
    public class Bracket : AutomaticallyGeneratableIdentifiable, Nameable, Describable {

        /// <summary>Name of this bracket</summary>
        public string Name { get; set; } = "";

        /// <summary>Description of this bracket</summary>
        public string Description { get; set; } = "";

        /// <summary>Rate income that falls within this bracket is taxed at</summary>
        public double Rate { get; set; } = 0.0;

        /// <summary>IncomeType this bracket applies to</summary>
        public IncomeType IncomeType { get; set; } = IncomeType.PERSONAL;

        /// <summary>Start of this bracket (inclusive)</summary>
        [Range(0, int.MaxValue)]
        public long Start { get; set; } = 0;

        /// <summary>End of this bracket (Non-inclusive)</summary>
        [Range(0, int.MaxValue)]
        public long End { get; set; } = long.MaxValue;

        /// <summary>Jurisdiction this bracket belongs to</summary>
        [JsonIgnore]
        public Jurisdiction? Jurisdiction { get; set; }
    }
}