using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using projectWebKassa.DAL;
using projectWebKassa.Models;
using System.Data;
using System.Data.Entity;


namespace projectWebKassa.BL
{

    public class ProductManagmentViewModel
    {

        public class ProductService
        {
            public string dBaseFoute = "";

            public List<CategorieViewModels> HaalAlleCategorieen()
            {
                List<CategorieViewModels> rettype = new List<CategorieViewModels>();

                try
                {
                    using (WebKassaContextContainer db = new WebKassaContextContainer())
                    {
                        var categorieen = db.categorieSet.OrderBy(n => n.Naam).ToList();

                        foreach (var cat in categorieen)
                        {
                            CategorieViewModels retCat = new CategorieViewModels()
                            {
                                CategorieID = cat.categorieId,
                                CategorieNaam = cat.Naam
                            };
                            rettype.Add(retCat);
                        }
                        return rettype;
                    }
                }
                catch (System.Data.Entity.Infrastructure.DbUpdateException ex)
                {
                    dBaseFoute = ex.Message;
                    return rettype;
                }
            }

            public CategorieViewModels HaalCategoriePer(string Naam)
            {
                CategorieViewModels retCat = new CategorieViewModels();

                try
                {
                    using (WebKassaContextContainer db = new WebKassaContextContainer())
                    {
                        var categorie = db.categorieSet.Find(Naam);
                        retCat = new CategorieViewModels()
                        {
                            categorieID = categorie.categorieId,
                            categorieNaam = categorie.Naam

                        };
                        return retCat;
                    }
                }
                catch (System.Data.Entity.Infrastructure.DbUpdateException ex)
                {
                    dBaseFoute = ex.Message;
                    return retCat;
                }
            }

            public CategorieViewModels HaalCategoriePer(int CategorieID)
            {
                CategorieViewModels retCat = new CategorieViewModels();
                try
                {
                    using (WebKassaContextContainer db = new WebKassaContextContainer())
                    {
                        var Categorie = db.categorieSet.Find(CategorieID);

                        retCat = new CategorieViewModels()
                        {
                            CategorieID = Categorie.categorieId,
                            CategorieNaam = Categorie.Naam
                        };
                        return retCat;
                    }
                }
                catch (System.Data.Entity.Infrastructure.DbUpdateException ex)
                {
                    dBaseFoute = ex.Message;
                    return retCat;
                }
            }
            public list<ProductViewModels> HaalAlleProductenPer(int CategorieID)
            {
                List<ProductViewModels> retType = new List<ProductViewModels>();
                try
                {
                    using (WebKassaContextContainer db = new WebKassaContextContainer())
                    {
                        var producten = db.productSet.Where(e => e.categorieId == CategorieID).ToList();

                        foreach (var Product in producten)
                        {
                            ProductViewModels retProd = new ProductViewModels()
                            {
                                CategorieID = product.CategorieID,
                                productID = product.ProductID,
                                ProductInfo = product.productInfo,
                                ProductNaam = product.ProductNaam,

                            };
                            retType.Add(retProd);
                        }
                        return retType;
                    }
                }
                catch (System.Data.Entity.Infrastructure.DbUpdateException ex)
                {
                    dBaseFoute = ex.Message;
                    return retType;
                }
            }
            public List<ProductViewModels> HaalAlleProductenPer(string CategorieNaam)
            {
                List<ProductViewModels> retType = new List<ProductViewModels>();

                try
                {
                    using (WebKassaContextContainer db = new WebKassaContextContainer())
                    {
                        var Producten = db.productSet.Where(e => e.categorie.Naam == CategorieNaam).ToList();
                        foreach (var product in Producten)
                        {
                            ProductViewModels retProd = new ProductViewModels()
                            {
                                CategorieID = Product.CategorieID,
                                ProductID = product.ProductID,
                                ProductInfo = product.ProductInfo,
                                productNaam = product.ProductNaam
                            };
                            retType.Add(retProd);
                        }
                        return retType;
                    }
                }
                catch (System.Data.Entity.Infrastructure.DbUpdateException ex)
                {
                    dBaseFoute = ex.Message;
                    return retType;
                }
            }

            public List<ProductViewModels> HaalAlleProducten()
            {
                List<ProductViewModels> retType = new List<ProductViewModels>();
                try
                {
                    using (WebKassaContextContainer db = new WebKassaContextContainer())
                    {
                        var producten = db.productSet.ToList();

                        foreach (var Product in producten)
                        {
                            ProductViewModels retProd = new ProductViewModels()
                            {
                                categorieID = product.CategrieID,
                                ProductID = product.ProductID,
                                ProductInfo = product.ProductInfo,
                                productNaam = product.ProductNaam
                            };
                            retType.Add(retProd);
                        }
                        return retType;
                    }
                }
                catch (System.Data.Entity.Infrastructure.DbUpdateException ex)
                {
                    dBaseFoute = ex.Message;
                    return retType;
                }
            }
            public List<PrijsViewModels> HaalAllePrijzenPer(int ProductID)
            {
                List<PrijsViewModels> retType = new List<PrijsViewModels>();
                try
                {
                    using (WebKassaContextContainer db = new WebKassaContextContainer())
                    {
                        var VerkoopPrijzen = db.prijsSet.Where(v => v.productId == ProductID).OrderBy(t => t.StartDatum).ToList();
                        foreach (var Prijs in VerkoopPrijzen)
                        {
                            PrijsViewModels retPrijs = new PrijsViewModels()
                            {
                                BTW = VerkoopPrijs.BTW,
                                prijs = verkoopPrijs.Prijs,
                                StartDatum = verkoopPrijs.StartDatum,
                                ProductID = verkoopPrijs.ProductID,
                                verkoopPrijsID = verkoopPrijs.PrijsID
                            };
                            retType.Add(retPrijs);
                        }
                        return retType;
                    }
                }
                catch (System.Data.Entity.Infrastructure.DbUpdateException ex)
                {
                    dBaseFoute = ex.Message;
                    return retType;
                }
            }
            public List<PrijsViewModels> HaalAllePrijzenPer(string ProductNaam)
            {
                List<PrijsViewModels> retType = new List<PrijsViewModels>();
                try
                {
                    using (WebKassaContextContainer db = new WebKassaContextContainer())
                    {
                        var verkoopPrijs = db.prijsSet.Where(v => v.product.Naam == ProductNaam).OrderBy(t => t.startDatum).ToList();

                        foreach (var verkoopPrijs in verkoopPrijs)
                        {
                            PrijsViewModels retPrijs = new PrijsViewModels()
                            {
                                BTW = verkoopPrijs.BTW,
                                Prijs = verkoopPrijs.Prijs,
                                StartDatum = verkoopPrijs.StartDatum,
                                ProductID = verkoopPrijs.productID,
                                VerkoopPrijsID = verkoopPrijs.VerkoopPrijsID
                            };
                            retType.Add(retPrijs);
                        }
                        return retType;
                    }
                }
                catch (System.Data.Entity.Infrastructure.DbUpdateException ex)
                {
                    dBaseFoute = ex.Message;
                    return retType;
                }
            }

            public ProductViewModels HaalproductPer(int ProductID)
            {
                ProductViewModels retType = new ProductViewModels();
                try
                {
                    using (WebKassaContextContainer db = new WebKassaContextContainer())
                    {
                        var product = db.productSet.Find(ProductID);

                        retType = new ProductViewModels()
                        {
                            CategorieID = product.categorieId,
                            CategorieNaam = product.categorie.categorieId,
                            productID = product.ProductId,
                            ProductInfo = product.ProductInfo,
                            ProductNaam = product.Naam

                        };
                        return retType;
                    }
                }
                catch (System.Data.Entity.Infrastructure.DbUpdateException ex)
                {
                    dBaseFoute = ex.Message;
                    return retType;
                }
            }
        }
    }
}

