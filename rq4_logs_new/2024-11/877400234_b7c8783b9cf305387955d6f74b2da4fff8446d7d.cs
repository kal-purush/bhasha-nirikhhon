using Newtonsoft.Json;
using NikiConnectAPI.Lib.Attributes;
using NikiConnectAPI.Lib.Interfaces;
using System;

namespace NikiConnectAPI.Lib.Models.SyncModels
{
    [System.ComponentModel.DisplayName("items")]
    public class Item : IBaseModel
    {
        [JsonProperty("id")]
        public int Id { get; set; }

        [JsonProperty("pim_item_id")]
        public int? PimItemId { get; set; }

        [JsonProperty("company_id")]
        public int CompanyId { get; set; }

        [JsonProperty("entity_account_id")]
        public int EntityAccountId { get; set; }

        [JsonProperty("tax_group_id")]
        public int TaxGroupId { get; set; }

        [JsonProperty("is_composed")]
        public bool IsComposed { get; set; }

        [JsonProperty("has_variants")]
        public bool HasVariants { get; set; }

        [JsonProperty("status_id")]
        public int StatusId { get; set; }

        [JsonProperty("attachment_id")]
        public int? AttachmentId { get; set; }

        [JsonProperty("external_id")]
        public string ExternalId { get; set; }

        [JsonProperty("category_id")]
        public object CategoryId { get; set; }

        [JsonProperty("family_id")]
        public int FamilyId { get; set; }

        [JsonProperty("item_type_id")]
        public int ItemTypeId { get; set; }

        [JsonProperty("barcode_type_id")]
        public int BarcodeTypeId { get; set; }

        [JsonProperty("barcode")]
        public string Barcode { get; set; }

        [JsonProperty("main_unit_id")]
        public int MainUnitId { get; set; }

        [JsonProperty("sale_unit_id")]
        public int SaleUnitId { get; set; }

        [JsonProperty("purchase_unit_id")]
        public int PurchaseUnitId { get; set; }

        [JsonProperty("matchcode")]
        public string Matchcode { get; set; }

        [JsonProperty("capacity")]
        public int Capacity { get; set; }

        [JsonProperty("capacity_unit_id")]
        public int CapacityUnitId { get; set; }

        [JsonProperty("hs_code_export_id")]
        public int? HsCodeExportId { get; set; }

        [JsonProperty("hs_code_import_id")]
        public int? HsCodeImportId { get; set; }

        [JsonProperty("width")]
        public double Width { get; set; }

        [JsonProperty("height")]
        public double Height { get; set; }

        [JsonProperty("length")]
        public double Length { get; set; }

        [JsonProperty("netweight")]
        public double Netweight { get; set; }

        [JsonProperty("grossweight")]
        public double Grossweight { get; set; }

        [JsonProperty("properties")]
        public Properties Properties { get; set; }

        [JsonProperty("sync_id")]
        public string SyncId { get; set; }

        [JsonProperty("revision_id")]
        public int RevisionId { get; set; }

        //[JsonProperty("created_by")]
        //public object CreatedBy { get; set; }

        //[JsonProperty("created_at")]
        //public DateTime CreatedAt { get; set; }

        //[JsonProperty("updated_by")]
        //public int UpdatedBy { get; set; }

        //[JsonProperty("updated_at")]
        //public DateTime UpdatedAt { get; set; }

        //[JsonProperty("deleted_by")]
        //public object DeletedBy { get; set; }

        //[JsonProperty("deleted_at")]
        //public object DeletedAt { get; set; }

        [JsonProperty("order_apply_round")]
        public bool OrderApplyRound { get; set; }

        [JsonProperty("order_round_percent")]
        public int OrderRoundPercent { get; set; }

        [JsonProperty("order_round_unit_id")]
        public int OrderRoundUnitId { get; set; }

        [JsonProperty("module_comments")]
        public object ModuleComments { get; set; }
    }

    public class Properties
    {
        [JsonProperty("validity_days")]
        public int ValidityDays { get; set; }

        [JsonProperty("consumption_days")]
        public int ConsumptionDays { get; set; }
    }
}