using EnterpriseLayers.Contract.DataAccess;
using EnterpriseLayers.Model.Domain;
using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EnterpriseLayers.Data.Context {
	public class MySqlEFContext : DbContext {
		public MySqlEFContext() : base("name=EnterpriseLayersData") {
			this.Database.Log = s => System.Diagnostics.Debug.WriteLine(s); //print EF sql output
			//EventLog.Log($"DbContext created. Status: {Database.Connection.State}");
		}

		public virtual DbSet<Person> Person { get; set; }
		public virtual DbSet<Customer> Customer { get; set; }
		public virtual DbSet<SalesOrderDetail> SalesOrderDetail { get; set; }
		public virtual DbSet<SalesOrderHeader> SalesOrderHeader { get; set; }
		public virtual DbSet<ContactType> ContactType { get; set; }
		public virtual DbSet<ProductModel> ProductModels { get; set; }
		public virtual DbSet<Illustration> Illustration { get; set; }
		public virtual DbSet<ProductModelIllustration> ProductModelIllustrations { get; set; }
	}
}