using System;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;

namespace Com.Danliris.Service.Sales.Lib.Migrations
{
    public partial class NewMigrationFix : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "ShipmentDocumentId",
                table: "SalesInvoices");

            migrationBuilder.DropColumn(
                name: "Amount",
                table: "SalesInvoiceDetails");

            migrationBuilder.DropColumn(
                name: "Price",
                table: "SalesInvoiceDetails");

            migrationBuilder.DropColumn(
                name: "ProductCode",
                table: "SalesInvoiceDetails");

            migrationBuilder.DropColumn(
                name: "ProductName",
                table: "SalesInvoiceDetails");

            migrationBuilder.DropColumn(
                name: "Quantity",
                table: "SalesInvoiceDetails");

            migrationBuilder.DropColumn(
                name: "SalesInvoiceId",
                table: "SalesInvoiceDetails");

            migrationBuilder.DropColumn(
                name: "Total",
                table: "SalesInvoiceDetails");

            migrationBuilder.DropColumn(
                name: "DOSalesId",
                table: "DOSalesLocalItems");

            migrationBuilder.RenameColumn(
                name: "ShipmentDocumentCode",
                table: "SalesInvoices",
                newName: "PaymentType");

            migrationBuilder.RenameColumn(
                name: "IDNo",
                table: "SalesInvoices",
                newName: "BuyerNIK");

            migrationBuilder.RenameColumn(
                name: "UomUnit",
                table: "SalesInvoiceDetails",
                newName: "BonNo");

            migrationBuilder.RenameColumn(
                name: "UomId",
                table: "SalesInvoiceDetails",
                newName: "ShippingOutId");

            migrationBuilder.AddColumn<string>(
                name: "BuyerCode",
                table: "SalesInvoices",
                maxLength: 255,
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "Sales",
                table: "SalesInvoices",
                maxLength: 256,
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "UnitCode",
                table: "SalesInvoices",
                maxLength: 1000,
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "UnitId",
                table: "SalesInvoices",
                maxLength: 100,
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "UnitName",
                table: "SalesInvoices",
                maxLength: 1000,
                nullable: true);

            migrationBuilder.AddColumn<int>(
                name: "SizeBreakdownIndex",
                table: "RO_Garment_SizeBreakdowns",
                nullable: false,
                defaultValue: 0);

            migrationBuilder.AddColumn<int>(
                name: "SizeBreakdownDetailIndex",
                table: "RO_Garment_SizeBreakdown_Details",
                nullable: false,
                defaultValue: 0);

            migrationBuilder.AddColumn<string>(
                name: "DOSalesCategory",
                table: "DOSales",
                maxLength: 255,
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "StorageCode",
                table: "DOSales",
                maxLength: 255,
                nullable: true);

            migrationBuilder.AddColumn<int>(
                name: "StorageId",
                table: "DOSales",
                nullable: false,
                defaultValue: 0);

            migrationBuilder.AddColumn<string>(
                name: "StorageName",
                table: "DOSales",
                maxLength: 255,
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "StorageUnit",
                table: "DOSales",
                maxLength: 255,
                nullable: true);

            migrationBuilder.AddColumn<int>(
                name: "MaterialIndex",
                table: "CostCalculationGarment_Materials",
                nullable: false,
                defaultValue: 0);

            migrationBuilder.CreateTable(
                name: "DOReturns",
                columns: table => new
                {
                    Id = table.Column<long>(nullable: false)
                        .Annotation("SqlServer:ValueGenerationStrategy", SqlServerValueGenerationStrategy.IdentityColumn),
                    Active = table.Column<bool>(nullable: false),
                    CreatedUtc = table.Column<DateTime>(nullable: false),
                    CreatedBy = table.Column<string>(maxLength: 255, nullable: false),
                    CreatedAgent = table.Column<string>(maxLength: 255, nullable: false),
                    LastModifiedUtc = table.Column<DateTime>(nullable: false),
                    LastModifiedBy = table.Column<string>(maxLength: 255, nullable: false),
                    LastModifiedAgent = table.Column<string>(maxLength: 255, nullable: false),
                    IsDeleted = table.Column<bool>(nullable: false),
                    DeletedUtc = table.Column<DateTime>(nullable: false),
                    DeletedBy = table.Column<string>(maxLength: 255, nullable: false),
                    DeletedAgent = table.Column<string>(maxLength: 255, nullable: false),
                    UId = table.Column<string>(maxLength: 255, nullable: true),
                    Code = table.Column<string>(maxLength: 255, nullable: true),
                    AutoIncreament = table.Column<long>(nullable: false),
                    DOReturnNo = table.Column<string>(maxLength: 255, nullable: true),
                    DOReturnType = table.Column<string>(maxLength: 255, nullable: true),
                    DOReturnDate = table.Column<DateTimeOffset>(nullable: false),
                    ReturnFromId = table.Column<int>(nullable: false),
                    ReturnFromName = table.Column<string>(maxLength: 255, nullable: true),
                    LTKPNo = table.Column<string>(maxLength: 255, nullable: true),
                    HeadOfStorage = table.Column<string>(maxLength: 255, nullable: true),
                    Remark = table.Column<string>(maxLength: 1000, nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_DOReturns", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "SalesInvoiceItems",
                columns: table => new
                {
                    Id = table.Column<long>(nullable: false)
                        .Annotation("SqlServer:ValueGenerationStrategy", SqlServerValueGenerationStrategy.IdentityColumn),
                    Active = table.Column<bool>(nullable: false),
                    CreatedUtc = table.Column<DateTime>(nullable: false),
                    CreatedBy = table.Column<string>(maxLength: 255, nullable: false),
                    CreatedAgent = table.Column<string>(maxLength: 255, nullable: false),
                    LastModifiedUtc = table.Column<DateTime>(nullable: false),
                    LastModifiedBy = table.Column<string>(maxLength: 255, nullable: false),
                    LastModifiedAgent = table.Column<string>(maxLength: 255, nullable: false),
                    IsDeleted = table.Column<bool>(nullable: false),
                    DeletedUtc = table.Column<DateTime>(nullable: false),
                    DeletedBy = table.Column<string>(maxLength: 255, nullable: false),
                    DeletedAgent = table.Column<string>(maxLength: 255, nullable: false),
                    UId = table.Column<string>(maxLength: 255, nullable: true),
                    ProductId = table.Column<int>(nullable: false),
                    ProductCode = table.Column<string>(maxLength: 255, nullable: true),
                    ProductName = table.Column<string>(maxLength: 255, nullable: true),
                    QuantityPacking = table.Column<double>(maxLength: 255, nullable: false),
                    PackingUom = table.Column<string>(maxLength: 255, nullable: true),
                    ItemUom = table.Column<string>(maxLength: 255, nullable: true),
                    QuantityItem = table.Column<double>(nullable: false),
                    Price = table.Column<double>(nullable: false),
                    Amount = table.Column<double>(nullable: false),
                    ConvertUnit = table.Column<string>(nullable: true),
                    ConvertValue = table.Column<double>(nullable: false),
                    SalesInvoiceDetailModelId = table.Column<long>(nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_SalesInvoiceItems", x => x.Id);
                    table.ForeignKey(
                        name: "FK_SalesInvoiceItems_SalesInvoiceDetails_SalesInvoiceDetailModelId",
                        column: x => x.SalesInvoiceDetailModelId,
                        principalTable: "SalesInvoiceDetails",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Restrict);
                });

            migrationBuilder.CreateTable(
                name: "DOReturnDetails",
                columns: table => new
                {
                    Id = table.Column<long>(nullable: false)
                        .Annotation("SqlServer:ValueGenerationStrategy", SqlServerValueGenerationStrategy.IdentityColumn),
                    Active = table.Column<bool>(nullable: false),
                    CreatedUtc = table.Column<DateTime>(nullable: false),
                    CreatedBy = table.Column<string>(maxLength: 255, nullable: false),
                    CreatedAgent = table.Column<string>(maxLength: 255, nullable: false),
                    LastModifiedUtc = table.Column<DateTime>(nullable: false),
                    LastModifiedBy = table.Column<string>(maxLength: 255, nullable: false),
                    LastModifiedAgent = table.Column<string>(maxLength: 255, nullable: false),
                    IsDeleted = table.Column<bool>(nullable: false),
                    DeletedUtc = table.Column<DateTime>(nullable: false),
                    DeletedBy = table.Column<string>(maxLength: 255, nullable: false),
                    DeletedAgent = table.Column<string>(maxLength: 255, nullable: false),
                    UId = table.Column<string>(maxLength: 255, nullable: true),
                    SalesInvoiceId = table.Column<int>(nullable: false),
                    SalesInvoiceNo = table.Column<string>(maxLength: 255, nullable: true),
                    DOReturnModelId = table.Column<long>(nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_DOReturnDetails", x => x.Id);
                    table.ForeignKey(
                        name: "FK_DOReturnDetails_DOReturns_DOReturnModelId",
                        column: x => x.DOReturnModelId,
                        principalTable: "DOReturns",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Restrict);
                });

            migrationBuilder.CreateTable(
                name: "DOReturnDetailItems",
                columns: table => new
                {
                    Id = table.Column<long>(nullable: false)
                        .Annotation("SqlServer:ValueGenerationStrategy", SqlServerValueGenerationStrategy.IdentityColumn),
                    Active = table.Column<bool>(nullable: false),
                    CreatedUtc = table.Column<DateTime>(nullable: false),
                    CreatedBy = table.Column<string>(maxLength: 255, nullable: false),
                    CreatedAgent = table.Column<string>(maxLength: 255, nullable: false),
                    LastModifiedUtc = table.Column<DateTime>(nullable: false),
                    LastModifiedBy = table.Column<string>(maxLength: 255, nullable: false),
                    LastModifiedAgent = table.Column<string>(maxLength: 255, nullable: false),
                    IsDeleted = table.Column<bool>(nullable: false),
                    DeletedUtc = table.Column<DateTime>(nullable: false),
                    DeletedBy = table.Column<string>(maxLength: 255, nullable: false),
                    DeletedAgent = table.Column<string>(maxLength: 255, nullable: false),
                    UId = table.Column<string>(maxLength: 255, nullable: true),
                    DOSalesId = table.Column<int>(nullable: false),
                    DOSalesNo = table.Column<string>(maxLength: 255, nullable: true),
                    DOReturnDetailModelId = table.Column<long>(nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_DOReturnDetailItems", x => x.Id);
                    table.ForeignKey(
                        name: "FK_DOReturnDetailItems_DOReturnDetails_DOReturnDetailModelId",
                        column: x => x.DOReturnDetailModelId,
                        principalTable: "DOReturnDetails",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Restrict);
                });

            migrationBuilder.CreateTable(
                name: "DOReturnItems",
                columns: table => new
                {
                    Id = table.Column<long>(nullable: false)
                        .Annotation("SqlServer:ValueGenerationStrategy", SqlServerValueGenerationStrategy.IdentityColumn),
                    Active = table.Column<bool>(nullable: false),
                    CreatedUtc = table.Column<DateTime>(nullable: false),
                    CreatedBy = table.Column<string>(maxLength: 255, nullable: false),
                    CreatedAgent = table.Column<string>(maxLength: 255, nullable: false),
                    LastModifiedUtc = table.Column<DateTime>(nullable: false),
                    LastModifiedBy = table.Column<string>(maxLength: 255, nullable: false),
                    LastModifiedAgent = table.Column<string>(maxLength: 255, nullable: false),
                    IsDeleted = table.Column<bool>(nullable: false),
                    DeletedUtc = table.Column<DateTime>(nullable: false),
                    DeletedBy = table.Column<string>(maxLength: 255, nullable: false),
                    DeletedAgent = table.Column<string>(maxLength: 255, nullable: false),
                    UId = table.Column<string>(maxLength: 255, nullable: true),
                    ShipmentDocumentId = table.Column<int>(nullable: false),
                    ShipmentDocumentCode = table.Column<string>(maxLength: 255, nullable: true),
                    ProductCode = table.Column<string>(maxLength: 255, nullable: true),
                    ProductName = table.Column<string>(maxLength: 255, nullable: true),
                    Quantity = table.Column<string>(maxLength: 255, nullable: true),
                    PackingUom = table.Column<string>(maxLength: 255, nullable: true),
                    UomId = table.Column<int>(nullable: false),
                    UomUnit = table.Column<string>(maxLength: 255, nullable: true),
                    Total = table.Column<double>(nullable: true),
                    DOReturnDetailModelId = table.Column<long>(nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_DOReturnItems", x => x.Id);
                    table.ForeignKey(
                        name: "FK_DOReturnItems_DOReturnDetails_DOReturnDetailModelId",
                        column: x => x.DOReturnDetailModelId,
                        principalTable: "DOReturnDetails",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Restrict);
                });

            migrationBuilder.CreateIndex(
                name: "IX_DOReturnDetailItems_DOReturnDetailModelId",
                table: "DOReturnDetailItems",
                column: "DOReturnDetailModelId");

            migrationBuilder.CreateIndex(
                name: "IX_DOReturnDetails_DOReturnModelId",
                table: "DOReturnDetails",
                column: "DOReturnModelId");

            migrationBuilder.CreateIndex(
                name: "IX_DOReturnItems_DOReturnDetailModelId",
                table: "DOReturnItems",
                column: "DOReturnDetailModelId");

            migrationBuilder.CreateIndex(
                name: "IX_SalesInvoiceItems_SalesInvoiceDetailModelId",
                table: "SalesInvoiceItems",
                column: "SalesInvoiceDetailModelId");
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "DOReturnDetailItems");

            migrationBuilder.DropTable(
                name: "DOReturnItems");

            migrationBuilder.DropTable(
                name: "SalesInvoiceItems");

            migrationBuilder.DropTable(
                name: "DOReturnDetails");

            migrationBuilder.DropTable(
                name: "DOReturns");

            migrationBuilder.DropColumn(
                name: "BuyerCode",
                table: "SalesInvoices");

            migrationBuilder.DropColumn(
                name: "Sales",
                table: "SalesInvoices");

            migrationBuilder.DropColumn(
                name: "UnitCode",
                table: "SalesInvoices");

            migrationBuilder.DropColumn(
                name: "UnitId",
                table: "SalesInvoices");

            migrationBuilder.DropColumn(
                name: "UnitName",
                table: "SalesInvoices");

            migrationBuilder.DropColumn(
                name: "SizeBreakdownIndex",
                table: "RO_Garment_SizeBreakdowns");

            migrationBuilder.DropColumn(
                name: "SizeBreakdownDetailIndex",
                table: "RO_Garment_SizeBreakdown_Details");

            migrationBuilder.DropColumn(
                name: "DOSalesCategory",
                table: "DOSales");

            migrationBuilder.DropColumn(
                name: "StorageCode",
                table: "DOSales");

            migrationBuilder.DropColumn(
                name: "StorageId",
                table: "DOSales");

            migrationBuilder.DropColumn(
                name: "StorageName",
                table: "DOSales");

            migrationBuilder.DropColumn(
                name: "StorageUnit",
                table: "DOSales");

            migrationBuilder.DropColumn(
                name: "MaterialIndex",
                table: "CostCalculationGarment_Materials");

            migrationBuilder.RenameColumn(
                name: "PaymentType",
                table: "SalesInvoices",
                newName: "ShipmentDocumentCode");

            migrationBuilder.RenameColumn(
                name: "BuyerNIK",
                table: "SalesInvoices",
                newName: "IDNo");

            migrationBuilder.RenameColumn(
                name: "ShippingOutId",
                table: "SalesInvoiceDetails",
                newName: "UomId");

            migrationBuilder.RenameColumn(
                name: "BonNo",
                table: "SalesInvoiceDetails",
                newName: "UomUnit");

            migrationBuilder.AddColumn<int>(
                name: "ShipmentDocumentId",
                table: "SalesInvoices",
                nullable: false,
                defaultValue: 0);

            migrationBuilder.AddColumn<double>(
                name: "Amount",
                table: "SalesInvoiceDetails",
                nullable: false,
                defaultValue: 0.0);

            migrationBuilder.AddColumn<double>(
                name: "Price",
                table: "SalesInvoiceDetails",
                nullable: false,
                defaultValue: 0.0);

            migrationBuilder.AddColumn<string>(
                name: "ProductCode",
                table: "SalesInvoiceDetails",
                maxLength: 255,
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "ProductName",
                table: "SalesInvoiceDetails",
                maxLength: 255,
                nullable: true);

            migrationBuilder.AddColumn<string>(
                name: "Quantity",
                table: "SalesInvoiceDetails",
                maxLength: 255,
                nullable: true);

            migrationBuilder.AddColumn<int>(
                name: "SalesInvoiceId",
                table: "SalesInvoiceDetails",
                nullable: false,
                defaultValue: 0);

            migrationBuilder.AddColumn<double>(
                name: "Total",
                table: "SalesInvoiceDetails",
                nullable: false,
                defaultValue: 0.0);

            migrationBuilder.AddColumn<int>(
                name: "DOSalesId",
                table: "DOSalesLocalItems",
                nullable: false,
                defaultValue: 0);
        }
    }
}