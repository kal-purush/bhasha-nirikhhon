namespace Igtampe.Neco.Common {

    /// <summary>Holds items for a ledger for certifying any action done in Neco</summary>
    public class CertifiedItem : AutomaticallyGeneratableIdentifiable {

        /// <summary>Certification text</summary>
        public string Text { get; set; } = "";

        /// <summary>Date this item was certified on</summary>
        public DateTime Date { get; set; } = DateTime.Now;

        /// <summary>User this item was certified by</summary>
        public User? CertifiedBy { get; set; }
    }
}