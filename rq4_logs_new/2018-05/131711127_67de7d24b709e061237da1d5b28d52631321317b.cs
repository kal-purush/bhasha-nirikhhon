using System;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;
using System.Text;

namespace DockService.Core.Models
{
	public class Container
	{
		[Key]
		[DatabaseGenerated(DatabaseGeneratedOption.Identity)]
		public Guid Id { get; set; }

		/// <summary>
		/// Gets or sets the serial shipping container code.
		/// </summary>
		public string SerialShippingContainerCode { get; set; }

		/// <summary>
		/// Gets or sets the products.
		/// </summary>
		public List<Product> Products { get; set; }
	}
}