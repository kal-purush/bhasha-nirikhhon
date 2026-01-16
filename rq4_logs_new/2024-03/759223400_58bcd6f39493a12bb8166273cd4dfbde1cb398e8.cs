using System;
using System.Collections.Generic;

namespace Soenneker.Extensions.PropertyInfo.Tests.Dtos;

public class Product
{
    private readonly IList<DateTime> _revisions = new List<DateTime>();

    public Product(int id)
    {
        //Id = id;
      //  Reviews = new Dictionary<int, string>();
    }

    //public int Id { get; }
    //public string Description { get; set; }
    //public Price Price { get; set; }
    //public Uri ImageUrl { get; set; }

    //public IDictionary<int, string> Reviews { get; }

    protected ICollection<DateTime> Revisions => _revisions;

    public ICollection<DateTime> GetRevisions => Revisions;

    public const int MaxRevisions = 10;
}