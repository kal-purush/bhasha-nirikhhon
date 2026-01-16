using Igtampe.Neco.Common.IDGenerators;
using Igtampe.Neco.Common.Income;
using Igtampe.Neco.Common.Taxes;
using System.ComponentModel.DataAnnotations.Schema;
using System.Text.Json.Serialization;

namespace Igtampe.Neco.Common.Banking {

    /// <summary>A Bank Account</summary>
    public class Account : ManuallyGeneratableIdentifiable<string>, Nameable, Locatable {

        private  static readonly IDGenerator<string> gen = new NumericalGenerator(9);

        /// <summary>Name of this bank account</summary>
        public string Name { get; set; } = "";

        /// <summary>Balance of this bank account</summary>
        public long Balance { get; set; } = 0;

        /// <summary>Indicates whether or not this bank account should be listed in the directory</summary>
        public bool PubliclyListed { get; set; } = false;

        /// <summary>Indicates whether or not this bank account is closed</summary>
        public bool Closed { get; set; } = false;

        /// <summary>Bank this account belongs to</summary>
        public Bank? Bank { get; set; }

        /// <summary>List of users with access to this bank account</summary>
        [JsonIgnore]
        public List<User> Owners { get; set; } = new();

        /// <summary>Address of the entity this bank account is tied to (IE: The main owner's, or corporation's address) <br/><br/> This is for taxation purposes</summary>
        public string? Address { get; set; }

        /// <summary>District of the entity this bank account is tied to (IE: The main owner's, or corporation's address) <br/><br/> This is for taxation purposes</summary>
        public Jurisdiction? Jurisdiction { get; set; }

        /// <summary>Type of income received by this bank account</summary>
        public IncomeType IncomeType { get; set; } = IncomeType.PERSONAL;

        /// <summary>List of all incomeitems tied to this bank account (used for tax reporting)</summary>
        [JsonIgnore]
        [NotMapped]
        public List<IncomeItem> IncomeItems { get {
                List<IncomeItem> Items = new();
                Items.AddRange(Airlines);
                Items.AddRange(Apartments);
                Items.AddRange(Businessses);
                Items.AddRange(Corporations);
                Items.AddRange(Hotels);
                return Items;
        }}

        /// <summary>Airlines this Bank Account receives income from</summary>
        [JsonIgnore]
        public List<Airline> Airlines { get; set; } = new();

        /// <summary>Apartments this bank account receives income from</summary>
        [JsonIgnore]
        public List<Apartment> Apartments { get; set; } = new();

        /// <summary>Businesses this bank account receives income from</summary>
        [JsonIgnore]
        public List<Business> Businessses { get; set; } = new();

        /// <summary>Corporations this bank account receives income from</summary>
        [JsonIgnore]
        public List<Corporation> Corporations { get; set; } = new();

        /// <summary>Hotels this bank account receives income from</summary>
        [JsonIgnore]
        public List<Hotel> Hotels { get; set; } = new();

        /// <summary>ID Generator for bank accounts</summary>
        [JsonIgnore]
        public override IDGenerator<string> IDGenerator => gen;

    }
}