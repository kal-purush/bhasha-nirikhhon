using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataParser.HelperClasses
{
    public static class JoinerArticles
    {
        public static IEnumerable<ProductCategoryObject> JoinInOrderEnumerable(IEnumerable<ProductCategoryObject> collection,
            string productID, string[] productFieldsForSingleProp = null, string[] productFieldsForPluralProp = null)
        {
            var similarProds = new List<ProductCategoryObject>();
            var prevProdID = string.Empty;

            foreach(var prod in collection)
            {
                if (prod.IsCategory)
                {
                    if (similarProds.Count > 0)
                    {
                        var prods = ReturnProducts(similarProds, productFieldsForSingleProp, productFieldsForPluralProp);

                        foreach (var e in prods)
                        {
                            yield return e;
                        }
                    }

                    yield return prod;
                }
                else
                {
                    if (prevProdID != prod.SingleProperties[productID] && similarProds.Count > 0)
                    {
                        var prods = ReturnProducts(similarProds, productFieldsForSingleProp, productFieldsForPluralProp);

                        foreach (var e in prods)
                        {
                            yield return e;
                        }
                    }

                    if (similarProds.Count == 0)
                    {
                        prevProdID = prod.SingleProperties[productID];
                    }
                    
                    similarProds.Add(prod);
                }
            }
        }

        static IEnumerable<ProductCategoryObject> ReturnProducts(
            List<ProductCategoryObject> similarProds,
            string[] productFieldsForSingleProp, string[] productFieldsForPluralProp)
        {
            productFieldsForPluralProp = productFieldsForPluralProp ?? new string[0];
            productFieldsForSingleProp = productFieldsForSingleProp ?? new string[0];

            foreach (var nextProd in similarProds.Skip(1))
            {
                foreach (var prop in productFieldsForSingleProp)
                {
                    similarProds[0].SingleProperties[prop] += nextProd.SingleProperties[prop];
                }

                foreach (var prop in productFieldsForPluralProp)
                {
                    similarProds[0].PluralProperties[prop] =
                        similarProds[0].PluralProperties[prop].Extend(nextProd.PluralProperties[prop])
                        .ToArray();
                }
            }

            yield return similarProds[0];

            foreach (var nextProd in similarProds.Skip(1))
            {
                yield return nextProd;
            }

            similarProds.Clear();
        }
    }
}