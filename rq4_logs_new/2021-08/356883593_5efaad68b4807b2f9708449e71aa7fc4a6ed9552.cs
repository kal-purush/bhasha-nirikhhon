using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Com.Bateeq.Service.Warehouse.Lib.Helpers;
using Com.Bateeq.Service.Warehouse.Lib.Interfaces;
using Com.Bateeq.Service.Warehouse.Lib.Interfaces.SPKInterfaces;
using Com.Bateeq.Service.Warehouse.Lib.Models.InventoryModel;
using Com.Bateeq.Service.Warehouse.Lib.Models.SPKDocsModel;
using Com.Bateeq.Service.Warehouse.Lib.ViewModels.NewIntegrationViewModel;
using Com.Bateeq.Service.Warehouse.Lib.ViewModels.SpkDocsViewModel;
using Com.Moonlay.Models;
using HashidsNet;
using Microsoft.EntityFrameworkCore;
using MongoDB.Bson;
using Newtonsoft.Json;

namespace Com.Bateeq.Service.Warehouse.Lib.Facades
{
    public class SPKDocsControllerFacade : ISPKDoc
    {
        private string USER_AGENT = "Facade";

        private readonly WarehouseDbContext dbContext;
        private readonly DbSet<Inventory> dbSet;
        private readonly IServiceProvider serviceProvider;

        public SPKDocsControllerFacade(IServiceProvider serviceProvider, WarehouseDbContext dbContext)
        {
            this.serviceProvider = serviceProvider;
            this.dbContext = dbContext;
            this.dbSet = dbContext.Set<Inventory>();
        }

        public async Task<int> Create(SPKDocsFromFinihsingOutsViewModel viewModel, string username, string token)
        {
            int Created = 0;

            using (var transaction = this.dbContext.Database.BeginTransaction())
            {
                try
                {
                    List<SPKDocsItem> sPKDocsItems = new List<SPKDocsItem>();
                    int itemIdx = 1;
                    foreach (var item in viewModel.Items)
                    {

                        // ambil product code, ambil first or default dari garment invoice detail yang product code dan no ro nya sama
                        // lalu ambil invoiceItemid,
                        // lalu ambil invoiceId, lalu ambil suplier

                        //string itemsUri = "items/finished-goods";
                        //var httpClient = (IHttpClientService)serviceProvider.GetService(typeof(IHttpClientService));
                        //var response = await httpClient.PostAsync($"{APIEndpoint.Core}{itemsUri}", new StringContent(JsonConvert.SerializeObject(item).ToString(), Encoding.UTF8, General.JsonMediaType));

                        //response.EnsureSuccessStatusCode();

                        var isDifferentSize = item.IsDifferentSize;
                        if (isDifferentSize == true)
                        {
                            int idx = 1;
                            foreach (var detail in item.Details)
                            {
                                var sizeId = detail.Size.Id;
                                var barcode = GenerateBarcode(idx, sizeId);
                                var itemx = GetItem(barcode);

                                if (itemx == null || itemx.Count() == 0) //barcode belum terdaftar, insert ke tabel items (BMS) terlebih dahulu
                                {
                                    ItemCoreViewModelUsername itemCore = new ItemCoreViewModelUsername
                                    {
                                        dataDestination = new List<ItemViewModelRead>
                                        {
                                           new ItemViewModelRead
                                           {
                                               ArticleRealizationOrder = viewModel.RONo,
                                               code = barcode,
                                               name = viewModel.Comodity.name,
                                               Size = item.Size.Size,
                                               Uom = item.Uom.Unit,
                                               ImagePath = viewModel.ImagePath,
                                               ImgFile = "",
                                               Tags = "",
                                               Remark = "",
                                               Description = "",
                                               _id = 0
                                           }
                                        },
                                        color = viewModel.color,
                                        process = viewModel.process,
                                        materials = viewModel.materials,
                                        materialCompositions = viewModel.materialCompositions,
                                        collections = viewModel.collections,
                                        seasons = viewModel.seasons,
                                        counters = viewModel.counters,
                                        subCounters = viewModel.subCounters,
                                        categories = viewModel.categories,
                                        DomesticCOGS = item.BasicPrice,
                                        DomesticRetail = 0,
                                        DomesticSale = item.BasicPrice + item.ComodityPrice,
                                        DomesticWholesale = 0,
                                        InternationalCOGS = 0,
                                        InternationalWholesale = 0,
                                        InternationalRetail = 0,
                                        InternationalSale = 0,
                                        ImageFile = "",
                                        _id = 0,
                                        Username = username,
                                        Token = token
                                    };

                                    string itemsUri = "items/finished-goods/item";
                                    var httpClient = (IHttpClientService)serviceProvider.GetService(typeof(IHttpClientService));
                                    var response = await httpClient.PostAsync($"{APIEndpoint.Core}{itemsUri}", new StringContent(JsonConvert.SerializeObject(itemCore).ToString(), Encoding.UTF8, General.JsonMediaType));

                                    response.EnsureSuccessStatusCode();

                                    var item2 = GetItem2(barcode);

                                    sPKDocsItems.Add(new SPKDocsItem
                                    {
                                        ItemArticleRealizationOrder = viewModel.RONo,
                                        ItemCode = barcode,
                                        ItemDomesticCOGS = item.BasicPrice,
                                        ItemDomesticSale = item.BasicPrice + item.ComodityPrice,
                                        ItemId = item2.Single()._id,
                                        ItemName = viewModel.Comodity.name,
                                        ItemSize = item.Size.Size,
                                        ItemUom = item.Uom.Unit,
                                        Quantity = item.Quantity,
                                        Remark = "",
                                        SendQuantity = item.Quantity,
                                    });

                                    var itemInInventory = dbContext.Inventories.Where(entity => entity.ItemCode == barcode && entity.StorageId == viewModel.UnitTo._id).FirstOrDefault();
                                    var itemId = item2.Single()._id;
                                    if (itemInInventory == null)
                                    {
                                        var inserted = await InsertToInventory(viewModel, item, barcode, itemId, username);
                                    }
                                    else
                                    {
                                        itemInInventory.Quantity = itemInInventory.Quantity + item.Quantity;
                                        EntityExtension.FlagForUpdate(itemInInventory, username, USER_AGENT);
                                        dbContext.Update(itemInInventory);
                                    }
                                }
                                else // barcode sudah terdaftar
                                {
                                    sPKDocsItems.Add(new SPKDocsItem
                                    {
                                        ItemArticleRealizationOrder = viewModel.RONo,
                                        ItemCode = barcode,
                                        ItemDomesticCOGS = item.BasicPrice,
                                        ItemDomesticSale = item.BasicPrice + item.ComodityPrice,
                                        ItemId = itemx.Single()._id,
                                        ItemName = viewModel.Comodity.name,
                                        ItemSize = item.Size.Size,
                                        ItemUom = item.Uom.Unit,
                                        Quantity = item.Quantity,
                                        Remark = "",
                                        SendQuantity = item.Quantity,
                                    });

                                    var itemInInventory = dbContext.Inventories.Where(entity => entity.ItemCode == barcode && entity.StorageId == viewModel.UnitTo._id).FirstOrDefault();
                                    var itemId = itemx.Single()._id;
                                    if (itemInInventory == null)
                                    {
                                        var inserted = await InsertToInventory(viewModel, item, barcode, itemId, username);
                                    }
                                    else
                                    {
                                        itemInInventory.Quantity = itemInInventory.Quantity + item.Quantity;
                                        EntityExtension.FlagForUpdate(itemInInventory, username, USER_AGENT);
                                        dbContext.Update(itemInInventory);
                                    }
                                }
                                idx++;
                            }
                        }
                        else
                        {
                            var sizeId = item.Size.Id;
                            var barcode = GenerateBarcode(itemIdx, sizeId);
                            var itemx = GetItem(barcode);

                            if (itemx == null || itemx.Count() == 0) //barcode belum terdaftar, insert ke tabel items (BMS) terlebih dahulu
                            {
                                ItemCoreViewModelUsername itemCore = new ItemCoreViewModelUsername
                                {
                                    dataDestination = new List<ItemViewModelRead>
                                        {
                                           new ItemViewModelRead
                                           {
                                               ArticleRealizationOrder = viewModel.RONo,
                                               code = barcode,
                                               name = viewModel.Comodity.name,
                                               Size = item.Size.Size,
                                               Uom = item.Uom.Unit,
                                               ImagePath = viewModel.ImagePath,
                                               ImgFile = "",
                                               Tags = "",
                                               Remark = "",
                                               Description = "",
                                               _id = 0
                                           }
                                        },
                                    color = viewModel.color,
                                    process = viewModel.process,
                                    materials = viewModel.materials,
                                    materialCompositions = viewModel.materialCompositions,
                                    collections = viewModel.collections,
                                    seasons = viewModel.seasons,
                                    counters = viewModel.counters,
                                    subCounters = viewModel.subCounters,
                                    categories = viewModel.categories,
                                    DomesticCOGS = item.BasicPrice,
                                    DomesticRetail = 0,
                                    DomesticSale = item.BasicPrice + item.ComodityPrice,
                                    DomesticWholesale = 0,
                                    InternationalCOGS = 0,
                                    InternationalWholesale = 0,
                                    InternationalRetail = 0,
                                    InternationalSale = 0,
                                    ImageFile = "",
                                    _id = 0,
                                    Username = username,
                                    Token = token
                                };

                                string itemsUri = "items/finished-goods/item";
                                var httpClient = (IHttpClientService)serviceProvider.GetService(typeof(IHttpClientService));
                                var response = await httpClient.PostAsync($"{APIEndpoint.Core}{itemsUri}", new StringContent(JsonConvert.SerializeObject(itemCore).ToString(), Encoding.UTF8, General.JsonMediaType));

                                response.EnsureSuccessStatusCode();

                                var item2 = GetItem2(barcode);

                                sPKDocsItems.Add(new SPKDocsItem
                                {
                                    ItemArticleRealizationOrder = viewModel.RONo,
                                    ItemCode = barcode,
                                    ItemDomesticCOGS = item.BasicPrice,
                                    ItemDomesticSale = item.BasicPrice + item.ComodityPrice,
                                    ItemId = item2.Single()._id,
                                    ItemName = viewModel.Comodity.name,
                                    ItemSize = item.Size.Size,
                                    ItemUom = item.Uom.Unit,
                                    Quantity = item.Quantity,
                                    Remark = "",
                                    SendQuantity = item.Quantity,
                                });

                                var itemInInventory = dbContext.Inventories.Where(entity => entity.ItemCode == barcode && entity.StorageId == viewModel.UnitTo._id).FirstOrDefault();
                                var itemId = item2.Single()._id;
                                if (itemInInventory == null)
                                {
                                    var inserted = await InsertToInventory(viewModel, item, barcode, itemId, username);
                                }
                                else
                                {
                                    itemInInventory.Quantity = itemInInventory.Quantity + item.Quantity;
                                    EntityExtension.FlagForUpdate(itemInInventory, username, USER_AGENT);
                                    dbContext.Update(itemInInventory);
                                }
                            }
                            else // barcode sudah terdaftar
                            {
                                sPKDocsItems.Add(new SPKDocsItem
                                {
                                    ItemArticleRealizationOrder = viewModel.RONo,
                                    ItemCode = barcode,
                                    ItemDomesticCOGS = item.BasicPrice,
                                    ItemDomesticSale = item.BasicPrice + item.ComodityPrice,
                                    ItemId = itemx.Single()._id,
                                    ItemName = viewModel.Comodity.name,
                                    ItemSize = item.Size.Size,
                                    ItemUom = item.Uom.Unit,
                                    Quantity = item.Quantity,
                                    Remark = "",
                                    SendQuantity = item.Quantity,
                                });

                                var itemInInventory = dbContext.Inventories.Where(entity => entity.ItemCode == barcode && entity.StorageId == viewModel.UnitTo._id).FirstOrDefault();
                                var itemId = itemx.Single()._id;
                                if (itemInInventory == null)
                                {
                                    var inserted = await InsertToInventory(viewModel, item, barcode, itemId, username);
                                }
                                else
                                {
                                    itemInInventory.Quantity = itemInInventory.Quantity + item.Quantity;
                                    EntityExtension.FlagForUpdate(itemInInventory, username, USER_AGENT);
                                    dbContext.Update(itemInInventory);
                                }
                            }
                            itemIdx++;
                        }
                    }

                    var packingListCode = GeneratePackingList();

                    SPKDocs data = new SPKDocs()
                    {
                        Code = GenerateCode("EFR-PK/PBJ"),
                        Date = viewModel.FinishingOutDate,
                        DestinationId = (long)viewModel.UnitTo._id,
                        DestinationCode = viewModel.UnitTo.code,
                        DestinationName = viewModel.UnitTo.name,
                        IsDistributed = true,
                        IsReceived = false,
                        PackingList = packingListCode,
                        Password = "1",
                        Reference = packingListCode,
                        SourceId = (long)viewModel.Unit._id,
                        SourceCode = viewModel.Unit.code,
                        SourceName = viewModel.Unit.name,
                        Weight = 0,
                        Items = sPKDocsItems
                    };

                    foreach (var i in data.Items)
                    {
                        EntityExtension.FlagForCreate(i, username, USER_AGENT);
                    }
                    EntityExtension.FlagForCreate(data, username, USER_AGENT);
                    dbContext.Add(data);

                    Created = await dbContext.SaveChangesAsync();
                    transaction.Commit();
                }
                catch (Exception e)
                {
                    transaction.Rollback();
                    throw new Exception(e.Message);
                }
            }

            return Created;
        }

        private List<ItemCoreViewModel> GetItem(string itemCode)
        {
            if (itemCode.Length < 5)
            {
                return null;
            }
            else
            {
                string itemUri = "items/finished-goods/Code";
                IHttpClientService httpClient = (IHttpClientService)serviceProvider.GetService(typeof(IHttpClientService));

                var response = httpClient.GetAsync($"{APIEndpoint.Core}{itemUri}/{itemCode}").Result;
                if (response.IsSuccessStatusCode)
                {
                    var content = response.Content.ReadAsStringAsync().Result;
                    Dictionary<string, object> result = JsonConvert.DeserializeObject<Dictionary<string, object>>(content);
                    List<ItemCoreViewModel> viewModel = JsonConvert.DeserializeObject<List<ItemCoreViewModel>>(result.GetValueOrDefault("data").ToString());
                    //return viewModel.OrderByDescending(s => s.Date).FirstOrDefault(s => s.Date < doDate.AddDays(1)); ;
                    return viewModel;
                }
                else
                {
                    return null;
                }
            }
        }

        private List<ItemCoreViewModel> GetItem2(string itemCode)
        {
            string itemUri = "items/finished-goods/Code";
            IHttpClientService httpClient = (IHttpClientService)serviceProvider.GetService(typeof(IHttpClientService));

            var response = httpClient.GetAsync($"{APIEndpoint.Core}{itemUri}/{itemCode}").Result;
            if (response.IsSuccessStatusCode)
            {
                var content = response.Content.ReadAsStringAsync().Result;
                Dictionary<string, object> result = JsonConvert.DeserializeObject<Dictionary<string, object>>(content);
                List<ItemCoreViewModel> viewModel = JsonConvert.DeserializeObject<List<ItemCoreViewModel>>(result.GetValueOrDefault("data").ToString());
                //return viewModel.OrderByDescending(s => s.Date).FirstOrDefault(s => s.Date < doDate.AddDays(1)); ;
                return viewModel;
            }
            else
            {
                return null;
            }
        }

        public string GenerateBarcode(int idx, int sizeId)
        {
            string code = "" + idx + sizeId;
            return code;
        }

        public async Task<int> InsertToInventory(SPKDocsFromFinihsingOutsViewModel viewModel, SPKDocItemsFromFinihsingOutsViewModel item, string barcode, long itemId, string username)
        {
            var Inserted = 0;

            Inventory inventory = new Inventory()
            {
                ItemArticleRealizationOrder = viewModel.RONo,
                ItemCode = barcode,
                ItemDomesticCOGS = item.BasicPrice,
                ItemDomesticSale = item.BasicPrice + item.ComodityPrice,
                ItemId = itemId,
                ItemName = viewModel.Comodity.name,
                ItemSize = item.Size.Size,
                ItemUom = item.Uom.Unit,
                Quantity = item.Quantity,
                ItemDomesticRetail = 0,
                ItemDomesticWholeSale = 0,
                ItemInternationalCOGS = 0,
                ItemInternationalRetail = 0,
                ItemInternationalSale = 0,
                ItemInternationalWholeSale = 0,
                StorageId = viewModel.UnitTo._id,
                StorageCode = viewModel.UnitTo.code,
                StorageName = viewModel.UnitTo.name,
                StorageIsCentral = false,
            };

            EntityExtension.FlagForCreate(inventory, username, USER_AGENT);
            dbContext.Add(inventory);
            return Inserted;
        }

        public string GeneratePackingList() // nomor urut/EFR-FN/bulan/tahun
        {
            var generatedNo = "";
            var date = DateTime.Now;
            var lastSPKDoc = dbContext.SPKDocs.OrderByDescending(entity => entity.Id).FirstOrDefault(entity => entity.PackingList.Contains("EFR-FN"));
            string lastPackingListCode = "";

            if (lastSPKDoc != null)
            {
                lastPackingListCode = lastSPKDoc.PackingList;
                var code = lastPackingListCode.Split("/");
                int nomorUrut = int.Parse(code.ElementAt(0));
                nomorUrut++;

                generatedNo = $"{nomorUrut.ToString("0000")}/EFR-FN/{date.ToString("MM")}/{date.ToString("yy")}";
            }
            else
            {
                generatedNo = $"0001/EFR-FN/{date.ToString("MM")}/{date.ToString("yy")}";
            }

            return generatedNo;
        }

        public string GenerateCode(string ModuleId)
        {
            var uid = ObjectId.GenerateNewId().ToString();
            var hashids = new Hashids(uid, 8, "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890");
            var now = DateTime.Now;
            var begin = new DateTime(DateTime.Now.Year, DateTime.Now.Month, 1);
            var diff = (now - begin).Milliseconds;
            string code = String.Format("{0}/{1}/{2}", hashids.Encode(diff), ModuleId, DateTime.Now.ToString("MM/yyyy"));
            return code;
        }

        public Tuple<List<SPKDocs>, int, Dictionary<string, string>> Read(int Page = 1, int Size = 25, string Order = "{}", string Keyword = null, string Filter = "{}")
        {
            throw new NotImplementedException();
        }

        public SPKDocs ReadById(int id)
        {
            throw new NotImplementedException();
        }

        public SPKDocs ReadByReference(string reference)
        {
            throw new NotImplementedException();
        }
    }
}