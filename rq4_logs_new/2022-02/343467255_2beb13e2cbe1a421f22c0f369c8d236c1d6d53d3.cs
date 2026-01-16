using System;
using Microsoft.EntityFrameworkCore.Migrations;

namespace ParkingApp.Main.DataAcces.Migrations
{
    public partial class InitialMigration : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "DrivingLicense",
                columns: table => new
                {
                    Id = table.Column<int>(type: "int", nullable: false)
                        .Annotation("SqlServer:Identity", "1, 1"),
                    Number = table.Column<string>(type: "nvarchar(12)", maxLength: 12, nullable: false),
                    ExpirationDate = table.Column<DateTime>(type: "datetime2", nullable: false),
                    CreatedOn = table.Column<DateTime>(type: "datetime2", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_DrivingLicense", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "ParkingAreas",
                columns: table => new
                {
                    Id = table.Column<int>(type: "int", nullable: false)
                        .Annotation("SqlServer:Identity", "1, 1"),
                    Emplacement = table.Column<string>(type: "nvarchar(max)", nullable: false),
                    TotalSpots = table.Column<int>(type: "int", nullable: false),
                    AvailableSpots = table.Column<int>(type: "int", nullable: true),
                    PricePerHour = table.Column<decimal>(type: "decimal(5,2)", nullable: false),
                    City = table.Column<string>(type: "nvarchar(max)", nullable: false),
                    EmplacementLocation = table.Column<string>(type: "nvarchar(max)", nullable: true),
                    CreatedOn = table.Column<DateTime>(type: "datetime2", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_ParkingAreas", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "User",
                columns: table => new
                {
                    Id = table.Column<int>(type: "int", nullable: false)
                        .Annotation("SqlServer:Identity", "1, 1"),
                    Name = table.Column<string>(type: "nvarchar(50)", maxLength: 50, nullable: false),
                    Email = table.Column<string>(type: "nvarchar(50)", maxLength: 50, nullable: false),
                    Phone = table.Column<string>(type: "nvarchar(12)", maxLength: 12, nullable: false),
                    Password = table.Column<string>(type: "nvarchar(max)", nullable: false),
                    Role = table.Column<int>(type: "int", nullable: false),
                    CreatedOn = table.Column<DateTime>(type: "datetime2", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_User", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "Admins",
                columns: table => new
                {
                    Id = table.Column<int>(type: "int", nullable: false)
                        .Annotation("SqlServer:Identity", "1, 1"),
                    UserId = table.Column<int>(type: "int", nullable: false),
                    ParkingAreaId = table.Column<int>(type: "int", nullable: true),
                    CreatedOn = table.Column<DateTime>(type: "datetime2", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Admins", x => x.Id);
                    table.ForeignKey(
                        name: "FK_Admins_ParkingAreas_ParkingAreaId",
                        column: x => x.ParkingAreaId,
                        principalTable: "ParkingAreas",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Restrict);
                    table.ForeignKey(
                        name: "FK_Admins_User_UserId",
                        column: x => x.UserId,
                        principalTable: "User",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "Drivers",
                columns: table => new
                {
                    Id = table.Column<int>(type: "int", nullable: false)
                        .Annotation("SqlServer:Identity", "1, 1"),
                    UserId = table.Column<int>(type: "int", nullable: false),
                    LicenseId = table.Column<int>(type: "int", nullable: true),
                    CreatedOn = table.Column<DateTime>(type: "datetime2", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Drivers", x => x.Id);
                    table.ForeignKey(
                        name: "FK_Drivers_DrivingLicense_LicenseId",
                        column: x => x.LicenseId,
                        principalTable: "DrivingLicense",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Restrict);
                    table.ForeignKey(
                        name: "FK_Drivers_User_UserId",
                        column: x => x.UserId,
                        principalTable: "User",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "Vehicles",
                columns: table => new
                {
                    Id = table.Column<int>(type: "int", nullable: false)
                        .Annotation("SqlServer:Identity", "1, 1"),
                    LicensePlate = table.Column<string>(type: "nvarchar(7)", maxLength: 7, nullable: false),
                    DriverId = table.Column<int>(type: "int", nullable: true),
                    CreatedOn = table.Column<DateTime>(type: "datetime2", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Vehicles", x => x.Id);
                    table.ForeignKey(
                        name: "FK_Vehicles_Drivers_DriverId",
                        column: x => x.DriverId,
                        principalTable: "Drivers",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Restrict);
                });

            migrationBuilder.CreateTable(
                name: "Reservations",
                columns: table => new
                {
                    Id = table.Column<int>(type: "int", nullable: false)
                        .Annotation("SqlServer:Identity", "1, 1"),
                    ReservationDate = table.Column<DateTime>(type: "datetime2", nullable: false),
                    StartTime = table.Column<DateTime>(type: "datetime2", nullable: false),
                    EndTime = table.Column<DateTime>(type: "datetime2", nullable: false),
                    Price = table.Column<decimal>(type: "decimal(5,2)", nullable: false),
                    IsPaid = table.Column<bool>(type: "bit", nullable: false),
                    State = table.Column<int>(type: "int", nullable: false),
                    VehicleId = table.Column<int>(type: "int", nullable: true),
                    ParkingAreaId = table.Column<int>(type: "int", nullable: true),
                    CreatedOn = table.Column<DateTime>(type: "datetime2", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Reservations", x => x.Id);
                    table.ForeignKey(
                        name: "FK_Reservations_ParkingAreas_ParkingAreaId",
                        column: x => x.ParkingAreaId,
                        principalTable: "ParkingAreas",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Restrict);
                    table.ForeignKey(
                        name: "FK_Reservations_Vehicles_VehicleId",
                        column: x => x.VehicleId,
                        principalTable: "Vehicles",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Restrict);
                });

            migrationBuilder.InsertData(
                table: "ParkingAreas",
                columns: new[] { "Id", "AvailableSpots", "City", "CreatedOn", "Emplacement", "EmplacementLocation", "PricePerHour", "TotalSpots" },
                values: new object[,]
                {
                    { 100, 96, "București", new DateTime(2022, 2, 1, 12, 26, 31, 887, DateTimeKind.Local).AddTicks(852), "Academiei", "Între bd. Regina Elisabeta și I. Campineanu", 10m, 96 },
                    { 262, 98, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(9523), "NICOLAE FILIPESCU", "INTRE STR. C.A. ROSETTI SI STR. BATISTEI", 10m, 98 },
                    { 263, 32, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(9614), "NICOLAE GOLESCU", "INTRE STR. G. ENESCU SI STR. EPISCOPIEI", 5m, 32 },
                    { 264, 35, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(9671), "NICOLAE IORGA", "INTRE CALEA VICTORIEI SI STR. HENRI COANDA", 5m, 35 },
                    { 265, 35, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(9737), "NICOLAE TITULESCU", "INTRE INTR. COJESTI SI STR. DR. IACOB FELIX", 2.5m, 35 },
                    { 266, 163, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(9787), "OCTAVIAN GOGA 1", "INTRE CALEA DUDESTI SI STR. NERVA TRAIAN", 2.5m, 163 },
                    { 267, 18, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(9827), "OCTAVIAN GOGA 3", "ARTERA DREAPTA PASAJ MARASESTI", 5m, 18 },
                    { 268, 138, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(9869), "OCTAVIAN GOGA 4", "DEASUPRA PASAJ MARASESTI", 5m, 138 },
                    { 269, 50, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(9909), "OCTAVIAN GOGA 5", "DEASUPRA PASAJ MARASESTI - DREAPTA PARCAJ GOGA 4", 5m, 50 },
                    { 270, 80, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(9949), "OPERA", "PTA. VICTOR BABES", 5m, 80 },
                    { 271, 12, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(9988), "ORIZONT", "INTR. JEAN GEORGESCU, LA INTERSECTIA CU STR. DEM I. DOBRESCU", 10m, 12 },
                    { 272, 28, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(29), "OTETELESANU", "INTRE STR. MATEI MILLO SI STR. C-TIN MILLE", 5m, 28 },
                    { 273, 11, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(69), "PANIPAT", "BD. BRATIANU", 10m, 11 },
                    { 274, 266, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(110), "PANTELIMON 1S", "SOS. MORARILOR - SOS. VERGULUI", 2m, 266 },
                    { 275, 61, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(149), "PECO ANGHEL SALIGNY", "STR. ANGHEL SALIGNY", 5m, 61 },
                    { 276, 72, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(190), "PECO BARIERE", "SP. INDEPENDENTEI, INTRE STR. A. SALIGNY SI ELIE RADU", 5m, 72 },
                    { 277, 25, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(250), "PECO M.I.", "INTERSECTIE A. SALIGNY - LIPSCANI", 5m, 25 },
                    { 278, 13, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(312), "PECO POMPIERI", "INTRE STR. A. SALIGNY SI ELIE RADU", 5m, 13 },
                    { 279, 99, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(400), "PESCARUS", "ALEEA DE ACCES IN PARCUL HERASTRAU - LA RESTAURANTUL PESCARUS", 5m, 99 },
                    { 280, 80, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(447), "PIATA ALBA IULIA 1", "INTRE BD. UNIRII SI BD. DECEBAL (ALVEOLE)", 2.5m, 80 },
                    { 281, 77, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(490), "PIATA AMZEI 1", "INTRE CALEA VICTORIEI SI STR. MENDELEEV", 5m, 77 },
                    { 282, 65, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(530), "PIATA AMZEI 2", "INTRE STR. BISERICA AMZEI SI PIATA AMZEI", 5m, 65 },
                    { 283, 47, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(569), "PIATA AMZEI 3", "INTRE STR. CHRISTIAN TELL SI PIATA AMZEI", 5m, 47 },
                    { 284, 37, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(610), "PIATA CHARLES DE GAULLE", "ALVEOLE PARTEA DREAPTA P-TA CH. DE GAULLE (BD. PREZAN)", 5m, 37 },
                    { 285, 494, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(648), "PIATA CONSTITUTIEI", "PIATA CONSTITUTIEI - PASTILA (FARA ALVEOLE)", 5m, 494 },
                    { 286, 25, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(691), "PIATA GEMENI", "PIATA GEMENI", 5m, 25 },
                    { 287, 19, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(730), "PIATA ITALIANA", "ITALIANA X STR. V. LASCAR X STR. C-TIN NACU", 5m, 19 },
                    { 288, 90, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(792), "PIATA LIBERTATII 1", "INTRE STR. C-TIN ISTRATI SI BD. MARASESTI (PARCUL CAROL)", 5m, 90 },
                    { 261, 156, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(9485), "NICOLAE CARAMFIL", "BD. NICOLAE CARANFIL", 10m, 156 },
                    { 260, 24, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(9445), "NICOLAE BALCESCU 2", "INTRE STR. C.A. ROSETTI SI STR. DEM. I. DOBRESCU", 10m, 24 },
                    { 259, 44, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(9403), "NICOLAE BALCESCU 1", "INTRE STR. DEM. I. DOBRESCU SI STR. I. CAMPINEANU", 10m, 44 },
                    { 258, 35, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(9364), "NERVA TRAIAN", "", 5m, 35 },
                    { 230, 73, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(7958), "ION CAMPINEANU", "INTRE CALEA VICTORIEI SI BD. N. BALCESCU", 10m, 73 },
                    { 231, 45, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(8090), "ION CAMPINEANU (SUPERB)", "INTRE STR. STIRBEI VODA SI STR. W. MARACINEANU", 5m, 45 },
                    { 232, 57, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(8143), "ION CAMPINEANU 1", "INTRE W.MARACINEANU SI CALEA VICTORIEI", 5m, 57 },
                    { 233, 24, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(8186), "ION GHICA", "INTRE I.C.BRATIANU SI DOAMNEI", 10m, 24 },
                    { 234, 46, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(8230), "JEAN LOUIS CALDERON 1", "INTRE ITALIANA SI C.A.ROSETTI", 10m, 46 },
                    { 235, 18, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(8298), "JEAN LOUIS CALDERON 2", "INTRE C.A.ROSETTI SI ITALIANA", 10m, 18 },
                    { 236, 49, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(8341), "JEAN LOUIS CALDERON 3", "INTRE ICOANEI SI PTA CANTACUZINO", 10m, 49 },
                    { 237, 84, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(8381), "KISELEFF ALVEOLA", "INTRE MONETARIEI SI ION MINCU", 5m, 84 },
                    { 238, 15, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(8421), "LASCAR CATARGIU – HOTEL MINERVA", "INTRE STR. H. COANDA SI STR. G-RAL GHE. MANU", 5m, 15 },
                    { 239, 44, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(8459), "LIBERTATII", "INTRE BD. NATIUNILE UNITE SI SPLAIUL INDEPENDENTEI, PE PARTE ADIACENTA PARCULUI", 5m, 44 }
                });

            migrationBuilder.InsertData(
                table: "ParkingAreas",
                columns: new[] { "Id", "AvailableSpots", "City", "CreatedOn", "Emplacement", "EmplacementLocation", "PricePerHour", "TotalSpots" },
                values: new object[,]
                {
                    { 240, 11, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(8502), "LIDO", "INTRE STR. ANASTASIE SINU SI STR. B. FRANKLIN", 5m, 11 },
                    { 241, 60, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(8542), "LIDO TERASA", "INTRE CALEA VICTORIEI SI BD. MAGHERU", 5m, 60 },
                    { 242, 27, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(8603), "LIPSCANI 1", "INTRE STR. A.SALIGNY SI STR. I.BREZOIANU", 5m, 27 },
                    { 289, 18, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(854), "PIATA LIBERTATII 2", "INTRE BD. MARASESTI SI STR. 11 IUNIE", 5m, 18 },
                    { 243, 30, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(8662), "LIPSCANI 2", "INTRE BD. I.C. BRATIANU SI CALEA MOSILOR", 5m, 30 },
                    { 245, 29, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(8745), "MAGAZIN VICTORIA", "MAGAZIN VICTORIA", 5m, 29 },
                    { 246, 62, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(8783), "MARIA ROSETTI", "INTRE STR ION LUCA CARAGIALE SI STR. SF SPIRIDON", 5m, 62 },
                    { 247, 43, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(8874), "MARRIOTT", "ZONA PTA. ARSENALULUI", 5m, 43 },
                    { 248, 31, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(8917), "MATEI BASARAB 1", "INTRE STR. MIRCEA VODA SI BD. CORNELIU COPOSU", 10m, 31 },
                    { 249, 55, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(8957), "MATEI MILLO", "INTRE CALEA VICTORIEI SI STR. G. VRACA", 5m, 55 },
                    { 250, 113, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(8998), "MENDELEEV", "STR. MENDELEEV, INTRE STR. G. ENESCU SI PTA. ROMANA", 5m, 113 },
                    { 251, 20, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(9036), "MINISTERUL MEDIULUI", "BD. LIBERTATII", 5m, 20 },
                    { 252, 319, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(9077), "MIRCEA VODA", "BD C. COPOSU - NERVA TRAIAN", 5m, 319 },
                    { 253, 20, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(9131), "BD. MIRCEA VODA, HOTEL MERCURE", "", 5m, 20 },
                    { 254, 29, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(9202), "BD. MIRCEA VODA, TIMPURI NOI", "INTERSECTIE BD. MIRCEA VODA SI STR. NERVA TRAIAN - PARC TIMPURI NOI", 5m, 29 },
                    { 255, 80, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(9245), "MIRCEA VULCANESCU", "INTRE STR. D.GOLESCU SI CALEA PLEVNEI", 5m, 80 },
                    { 256, 88, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(9285), "MIRCEA VULCANESCU 1", "INTRE CALEA GRIVITEI SI STR. HORATIU", 5m, 88 },
                    { 257, 29, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(9324), "NATIUNILE UNITE", "INTRE PTA. NATIUNILE UNITE SI STR. SF. APOSTOLI", 10m, 29 },
                    { 244, 53, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(8705), "LUTERANA", "INTRE STR. STIRBEI VODA SI STR. G-RAL BERTHELOT", 5m, 53 },
                    { 290, 136, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(896), "PIATA REVOLUTIEI", "INTRE STR. ION CAMPINEANU SI STR. DEM I. DOBRESCU", 5m, 136 },
                    { 291, 93, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(935), "PIATA VICTORIEI", "PTA. VICTORIEI (DEASUPRA PASAJULUI RUTIER)", 5m, 93 },
                    { 292, 10, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(977), "PIATA VICTORIEI SPRING TIME", "PTA. VICTORIEI", 5m, 10 },
                    { 325, 39, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(2487), "SOS. NORDULUI – AMBASADA CHINEI", "INTRE BD. BEIJING SI ALEEA PRIPORULUI", 5m, 39 },
                    { 326, 39, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(2528), "SPERANTEI", "INTRE STR. AR. CALINESCU SI CAROL I", 5m, 39 },
                    { 327, 24, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(2611), "SPLAI EXIM BANK ALVEOLA", "SP. INDEPENDENTEI", 5m, 24 },
                    { 328, 44, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(2659), "SPLAI TURN", "PE SP. INDEPENDENTEI, LA INTERSECTIA CU BD. NATIUNILE UNITE", 5m, 44 },
                    { 329, 14, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(2700), "SPLAIUL INDEPENDENTEI 3", "INTRE STR. GRADINA CU CAI SI STR. B.P.HASDEU", 5m, 14 },
                    { 330, 23, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(2742), "SPLAIUL INDEPENDENTEI 4", "INTRE BD. SCHITU MAGUREANU SI GRADINA CU CAI", 5m, 23 },
                    { 331, 23, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(2781), "SPLAIUL INDEPENDENTEI 5", "INTRE STR. ILFOV SI STR. MIHAI VODA", 5m, 23 },
                    { 332, 20, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(2826), "SPLAIUL INDEPENDENTEI 6", "INTRE CALEA VICTORIEI SI STR. ILFOV", 5m, 20 },
                    { 333, 167, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(2867), "SPLAIUL UNIRII", "INTRE STR. D. CANTEMIR SI BD. MARASESTI", 5m, 167 },
                    { 334, 322, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(2909), "STEFAN CEL MARE 1", "INTRE BUCUR OBOR SI VASILE LASCAR", 2.5m, 322 },
                    { 335, 260, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(2950), "STEFAN CEL MARE 2", "INTRE VASILE LASCAR SI CALEA FLOREASCA", 2.5m, 260 },
                    { 336, 38, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(2991), "TAKE IONESCU", "INTRE BD. GHE. MAGHERU SI STR. MENDELEEV", 5m, 38 },
                    { 337, 14, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(3031), "TERASA SIDNEY", "INTRE CALEA VICTORIEI SI LASCAR CATARGIU", 5m, 14 },
                    { 324, 384, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(2448), "SOS. NORDULUI", "INTRE BD. BEIJING SI STR. GRIGORE GAFENCU", 5m, 384 },
                    { 338, 56, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(3075), "TERASA SIDNEY 2", "INTRE CALEA VICTORIEI SI STR. BUZESTI", 5m, 56 },
                    { 340, 41, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(3157), "TONITZA", "ZONA DELIMITATA DE STR. NICOLAE TONITZA, STR. FILITTI SI SP. INDEPENDENTEI", 10m, 41 },
                    { 341, 14, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(3199), "TRAIAN", "INTRE STR. PLANTELOR SI STR. ST. MIHAILEANU", 5m, 14 },
                    { 342, 32, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(3240), "TRAIAN VUIA", "INTRE BD. N. BALCESCU SI STR. VASILE CONTA", 10m, 32 },
                    { 343, 125, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(3282), "TUDOR ARGHEZI", "", 10m, 125 },
                    { 344, 20, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(3376), "UDRISTE – STRADA", "PE STR. MATEI BASARAB, INTRE BD. CORNELIU COPOSU SI BD. MIRCEA VODA", 5m, 20 }
                });

            migrationBuilder.InsertData(
                table: "ParkingAreas",
                columns: new[] { "Id", "AvailableSpots", "City", "CreatedOn", "Emplacement", "EmplacementLocation", "PricePerHour", "TotalSpots" },
                values: new object[,]
                {
                    { 345, 30, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(3420), "UNIREA PANTA", "BD. CORNELIU COPOSU, INTRE BD. I.C.BRATIANU SI STR. SF. VINERI", 10m, 30 },
                    { 346, 14, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(3462), "UNIREA PASAJ", "BD. I.C.BRATIANU", 10m, 14 },
                    { 347, 211, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(3503), "UNIRII", "BD. UNIRII (PTA. CONSTITUTIEI - PTA. UNIRII)", 5m, 211 },
                    { 348, 171, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(3546), "BD. UNIRII – ALVEOLA 1D", "INTRE PIATA UNIRII SI MIRCEA VODA", 10m, 171 },
                    { 349, 247, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(3587), "BD. UNIRII – ALVEOLA 1S", "INTRE PTA UNIRII SI MIRCEA VODA", 10m, 247 },
                    { 350, 34, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(3629), "BD. UNIRII – ALVEOLA 2D", "INTRE MIRCEA VODA SI NERVA TRAIAN", 5m, 34 },
                    { 351, 124, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(3670), "BD. UNIRII – ALVEOLA 2S", "INTRE MIRCEA VODA SI BD. UNIRII TRONSON S3", 5m, 124 },
                    { 352, 12, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(3711), "CONSTANTIN ESARCU", "INTRE STR. EPISCOPIEI SI STR. NICOLAE GOLESCU", 5m, 12 },
                    { 339, 36, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(3114), "THOMAS MASARYK", "INTRE STR. VASILE LASCAR SI STR. J.L. CALDERON", 5m, 36 },
                    { 229, 52, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(7920), "ION BREZOIANU 3", "INTRE WALTER MARACINEANU SI POIANA NARCISELOR", 5m, 52 },
                    { 323, 42, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(2406), "SLANIC", "", 10m, 42 },
                    { 321, 13, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(2327), "SFINTII VOIEVOZI", "LA INTERSECTIA STR. SF. VOIEVOZI SI MIHAIL MOXA", 5m, 13 },
                    { 293, 127, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(1016), "PIATA WALTER MARACINEANU", "", 5m, 127 },
                    { 294, 52, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(1060), "PITAR MOS", "INTRE C.A.ROSETTI SI J.MICHELET", 5m, 52 },
                    { 295, 27, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(1166), "POIANA NARCISELOR 1", "INTRE STR. I.CAMPINEANU SI STR. I.BREZOIANU", 5m, 27 },
                    { 296, 40, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(1216), "POIANA NARCISELOR 2", "INTRE STR. STIRBEI VODA SI STR. VASILE SION", 5m, 40 },
                    { 297, 23, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(1267), "POLIZU", "INTRE STR. DR. VARNALI SI STR. IACOB FELIX", 5m, 23 },
                    { 298, 79, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(1338), "POLONA 1", "INTRE ION BOGDAN SI BROSTEANU", 5m, 79 },
                    { 299, 49, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(1383), "POLONA 2", "INTRE ION BOGDAN SI STEFAN CEL MARE", 5m, 49 },
                    { 300, 41, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(1424), "POMPILIU ELIADE", "INTRE PTA. M. KOGALNICEANU SI STR. SF. CONSTANTIN", 5m, 41 },
                    { 301, 160, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(1465), "POPA TATU", "INTRE STR. STIRBEI VODA SI STR. BERZEI", 5m, 160 },
                    { 302, 15, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(1504), "POSTA VITAN 1", "INTRE STR. O.GOGA SI CALEA VITAN", 2.5m, 15 },
                    { 303, 208, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(1545), "PIATA ALBA IULIA", "ROND PTA. ALBA IULIA", 2.5m, 208 },
                    { 304, 23, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(1587), "PIATA LAHOVARI", "PIATA LAHOVARI", 5m, 23 },
                    { 305, 527, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(1627), "PIATA PRESEI LIBERE", "ALEILE INCONJURATOARE CASA PRESEI", 2.5m, 527 },
                    { 322, 29, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(2368), "SILFIDELOR", "", 5m, 29 },
                    { 306, 108, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(1668), "RADU BELLER", "STR. AV. RADU BELLER", 2.5m, 108 },
                    { 308, 16, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(1748), "REGINA ELISABETA", "BD. REGINA ELISABETA", 5m, 16 },
                    { 309, 25, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(1788), "RIGAS", "INTRE STR. BREZOIANU SI PARC CISMIGIU", 5m, 25 },
                    { 310, 25, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(1829), "ROND PESCARUS", "ALVEOLA PARC HERASTRAU (BD. AVIATORILOR - BD. BEIJING)", 5m, 25 },
                    { 311, 26, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(1914), "ROSETTI SEMAFOR", "INTRE PRAPORGESCU SI J.L.CALDERON", 10m, 26 },
                    { 312, 166, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(1962), "SALA PALATULUI", "ALEILE DIN JURUL SALII PALATULUI", 5m, 166 },
                    { 313, 140, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(2002), "SCHITU MAGUREANU", "BD. SCHITU MAGUREANU", 5m, 140 },
                    { 314, 24, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(2043), "SCHITU MAGUREANU 2", "BD. SCHITU MAGUREANU", 5m, 24 },
                    { 315, 4, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(2084), "SCOALEI", "", 10m, 4 },
                    { 316, 110, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(2126), "SEVASTOPOL", "", 5m, 110 },
                    { 317, 117, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(2167), "SFANTA VINERI (STRADAL)", "INTRE BD. MIRCEA VODA SI STR. MAMULARI", 5m, 117 },
                    { 318, 160, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(2207), "SFANTA VINERI 2", "ADICENT BD. MIRCEA VODA", 5m, 160 },
                    { 319, 45, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(2246), "SFANTUL CONSTANTIN", "INTRE STR. POMPILIU ELIADE SI CALEA PLEVNEI", 5m, 45 },
                    { 320, 82, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(2287), "SFINTII APOSTOLI", "", 5m, 82 },
                    { 307, 24, "București", new DateTime(2022, 2, 1, 12, 26, 31, 889, DateTimeKind.Local).AddTicks(1707), "RAUREANU", "INTRE CALEA VICTORIEI SI SPLAIUL INDEPENDENTEI", 5m, 24 },
                    { 228, 19, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(7880), "ION BREZOIANU 2", "INTRE LIPSCANI SI DOMNITA ANASTASIA", 5m, 19 }
                });

            migrationBuilder.InsertData(
                table: "ParkingAreas",
                columns: new[] { "Id", "AvailableSpots", "City", "CreatedOn", "Emplacement", "EmplacementLocation", "PricePerHour", "TotalSpots" },
                values: new object[,]
                {
                    { 227, 47, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(7840), "ION BREZOIANU", "INTRE BD. REGINA ELISABETA SI INTR. RIGAS", 5m, 47 },
                    { 163, 126, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(4657), "CINA", "INTRE STR. C.A. ROSETTI SI STR. B. FRANKLIN", 5m, 126 },
                    { 135, 94, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(3336), "CADEREA BASTILIEI 1", "INTRE BD. LASCAR CATARGIU SI STR. I. SLATINEANU", 5m, 94 },
                    { 136, 57, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(3379), "CADEREA BASTILIEI 2", "INTRE BD. I. SLATINEANU SI BD. IANCU DE HUNEDOARA", 5m, 57 },
                    { 137, 125, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(3422), "CALEA FLOREASCA 1", "INTRE SOS. STEFAN CEL MARE SI STR. TUDOR VIANU", 2.5m, 125 },
                    { 138, 44, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(3466), "CALEA FLOREASCA 2", "INTRE STR. TUDOR VIANU SI STR. BALANESCU ROSETTI", 2.5m, 44 },
                    { 139, 63, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(3507), "CALEA FLOREASCA 3", "INTRE STR. BALANESCU ROSETTI SI STR. MIHAIL GLINKA", 2.5m, 63 },
                    { 140, 36, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(3550), "CALEA FLOREASCA 4", "INTRE STR. MIHAIL GLINKA SI STR. CEAIKOVSKI", 2.5m, 36 },
                    { 141, 53, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(3603), "CALEA FLOREASCA 5", "INTRE STR. CEAIKOVSKI SI STR. ANCUTA BANEASA", 2.5m, 53 },
                    { 142, 34, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(3672), "CALEA GRIVITEI 1", "INTRE PASAJ BASARAB SI VASILE STR. TARUSANU", 2.5m, 34 },
                    { 143, 71, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(3720), "CALEA GRIVITEI 2", "INTRE STR. POLIZU SI STR. SF. VOIEVOZI", 5m, 71 },
                    { 144, 40, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(3830), "CALEA GRIVITEI 3", "INTRE STR. SF. VOIEVOZI SI BD. DACIA", 5m, 40 },
                    { 145, 61, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(3877), "CALEA GRIVITEI 4", "INTRE BD. DACIA SI CALEA VICTORIEI", 5m, 61 },
                    { 146, 15, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(3917), "CALEA MOSILOR", "INTRE STR. BARATIEI SI STR. SF. VINERI", 10m, 15 },
                    { 147, 33, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(3956), "CALEA PLEVNEI", "CALEA PLEVNEI", 5m, 33 },
                    { 148, 85, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(3994), "CALEA PLEVNEI 1", "INTRE PTA. M.KOGALNICEANU SI STR. SF.CONSTANTIN", 5m, 85 },
                    { 149, 76, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(4031), "CALEA PLEVNEI 2", "INTRE STR. BERZEI SI STR. STIRBEI VODA", 2.5m, 76 },
                    { 150, 27, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(4071), "CALEA PLEVNEI 3", "INTRE STR. MIRCEA VULCANESCU SI STR. WITING", 2.5m, 27 },
                    { 151, 52, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(4110), "CALEA PLEVNEI 4", "INTRE STR. WITING SI STR. PISONI", 2.5m, 52 },
                    { 152, 48, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(4165), "CALEA PLEVNEI 5", "INTRE STR. PISONI SI STR. BARIEREI", 2.5m, 48 },
                    { 153, 12, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(4229), "CALEA PLEVNEI 6", "INTRE STR. BARIEREI SI STR. AFLUENTULUI", 2.5m, 12 },
                    { 154, 107, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(4271), "CALEA RAHOVEI", "INTRE BD. LIBERTATII SI STR. SABINELOR", 2.5m, 107 },
                    { 155, 14, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(4310), "CALEA VICTORIEI 1", "INTRE PTA. VICTORIEI SI STR. SEVASTOPOL", 5m, 14 },
                    { 156, 31, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(4349), "CALEA VICTORIEI 2", "INTRE CALEA GRIVITEI SI STR. BANULUI", 5m, 31 },
                    { 157, 475, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(4388), "CANDIANO POPESCU", "INTRE PTA. LIBERTATII SI CALEA SERBAN VODA", 2.5m, 475 },
                    { 158, 20, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(4426), "CEC BANCOREX", "STR. MIHAI VODA, INTRE CALEA VICTORIEI SI STR. ILFOV", 5m, 20 },
                    { 159, 60, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(4463), "CEC JUDECATORIE", "STR. ILFOV, INTRE SPLAI INDEP. SI STR. MIHAI VODA", 5m, 60 },
                    { 160, 30, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(4533), "CEC MARCONI", "STR. MARCONI, INTRE C.VICTORIEI SI STR. ILFOV", 5m, 30 },
                    { 161, 138, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(4579), "CHIRISTIGIILOR", "STR. CHIRISTIGIILOR, CF. AVIZ CTC NR. 11987/05.08.2010", 2.5m, 138 },
                    { 134, 25, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(3294), "C.A. ROSETTI", "INTRE BD. N. BALCESCU SI STR. T. ARGHEZI", 5m, 25 },
                    { 133, 198, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(3250), "BUCUR OBOR", "PE STR. HALELE OBOR", 2.5m, 198 },
                    { 132, 49, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(3206), "BOTEANU", "INTRE STR. C.A. ROSETTI SI STR. D.I. DOBRESCU", 5m, 49 },
                    { 131, 82, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(3161), "BISERICA ENEI", "INTRE STR. ACADEMIEI SI STR. EDGAR QUINET", 10m, 82 },
                    { 103, 59, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(1288), "ALEEA PRIVIGHETORILOR 1", "INTRE AL.PRIVIGHETORILOR SI SERBAN CANTACUZINO", 2.5m, 59 },
                    { 104, 56, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(1335), "ALEXANDRU BELDIMAN", "INTRE BD. REGINA ELISABETA SI STR. EFORIE", 5m, 56 },
                    { 105, 153, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(1390), "ALEXANDRU CONSTANTINESCU", "INTRE BD. MARASESTI SI BD. ION MIHALACHE", 2.5m, 153 },
                    { 106, 57, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(1466), "ALEXANDRU IOAN CUZA 1", "INTRE GH. DUCA SI DR. I. FELIX", 5m, 57 },
                    { 107, 47, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(1545), "ALEXANDRU IOAN CUZA 2", "INTRE GH. DUCA SI C-TIN DISESCU", 5m, 47 },
                    { 108, 76, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(1592), "ALEXANDRU IOAN CUZA 3", "INTRE SOS. N.TITULESCU SI STR. C-TIN DISESCU", 5m, 76 },
                    { 109, 7, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(1640), "ARISTIDE DEMETRIADE", "INTRE STR. BREZOIANU SI STR. GEORGE VRACA", 5m, 7 },
                    { 110, 41, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(1682), "PICTOR ARTHUR VERONA", "STR. PICTOR VERONA, INTRE BD. MAGHERU SI STR. PITAR MOS", 5m, 41 },
                    { 111, 70, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(1723), "ARTHUR VERONA 2", "INTRE PITAR MOS SI J.L.CALDERON", 5m, 70 }
                });

            migrationBuilder.InsertData(
                table: "ParkingAreas",
                columns: new[] { "Id", "AvailableSpots", "City", "CreatedOn", "Emplacement", "EmplacementLocation", "PricePerHour", "TotalSpots" },
                values: new object[,]
                {
                    { 112, 223, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(1815), "AVIATORILOR 1D", "INTRE ROND PESCARUS SI PIATA CHARLES DE GAULLE", 5m, 223 },
                    { 113, 181, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(1859), "AVIATORILOR 1S", "INTRE ROND PESCARUS SI PIATA CHARLES DE GAULLE", 5m, 181 },
                    { 114, 175, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(2003), "AVIATORILOR 2D", "INTRE PIATA CHARLES DE GAULLE SI GH. DEMETRIADE", 5m, 175 },
                    { 115, 181, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(2068), "AVIATORILOR 2S", "INTRE PTA CHARLES DE GAULLE SI STR. GH. DEMETRIADE", 5m, 181 },
                    { 162, 21, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(4620), "CHRISTIAN TELL", "INTRE STR. BISERICA AMZEI SI PIATA AMZEI", 5m, 21 },
                    { 116, 85, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(2115), "AVIATORILOR 3S", "INTRE PIATA VICTORIEI SI STR. CAPITAN GHE. DEMETRIADE", 5m, 85 },
                    { 118, 159, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(2227), "AVRIG 2", "INTRE SOS. IANCULUI SI STR. RITMULUI", 2.5m, 159 },
                    { 119, 23, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(2275), "BALANESCU ROSETTI", "INTRE CALEA FLOREASCA SI STR. BANUL ANTONACHE", 2.5m, 23 },
                    { 120, 65, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(2318), "BANU MANTA 1", "INTRE STR. G-RAL VLADOIANU BARBU SI STR. FROSA SARANDY", 2.5m, 65 },
                    { 121, 65, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(2361), "BANU MANTA 2", "INTRE STR. FROSA SARANDY SI BD. I. MIHALACHE", 2.5m, 65 },
                    { 122, 126, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(2404), "BANUL ANTONACHE", "", 2.5m, 126 },
                    { 123, 68, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(2446), "BARBU DELAVRANCEA", "INTRE BD. ION MIHALACHE SI STR. ION MINCU", 5m, 68 },
                    { 124, 495, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(2498), "BASARABIA 1", "INTRE PIATA MUNCII SI BD. CHISINAU", 2.5m, 495 },
                    { 125, 58, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(2573), "BATISTEI", "INTRE BD. NICOLAE BALCESCU SI STR. I.L.CARAGIALE", 10m, 58 },
                    { 126, 22, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(2621), "BCU – BIBLIOTECA CENTRALA UNIVERSITARA", "STR. DEM. I. DOBRESCU, INTRE CALEA VICTORIEI SI STR. BOTEANU", 5m, 22 },
                    { 127, 36, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(2843), "GENERAL BERTHELOT", "INTRE STR. L.CAZAVILLIAN SI STR. POPA TATU", 5m, 36 },
                    { 128, 57, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(2925), "GENERAL BERTHELOT 2", "INTRE STR. LUTERANA SI CALEA VICTORIEI", 5m, 57 },
                    { 129, 89, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(3030), "GENERAL BERTHELOT 3", "INTRE STR. POPA TATU SI STR. BERZEI", 5m, 89 },
                    { 130, 59, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(3108), "BIBESCU VODA", "INTRE SP. INDEPENDENTEI SI BD. D. CANTEMIR", 5m, 59 },
                    { 117, 145, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(2159), "AVRIG 1", "INTRE BD. FERDINAND SI STR. CHIRISTIGIILOR", 2.5m, 145 },
                    { 226, 12, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(7801), "IANCU DE HUNEDOARA 2", "INTRE INTR. GENEVA SI STR. CLOPOTARII VECHI", 5m, 12 },
                    { 164, 72, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(4718), "CINA 2", "INTRE STR. B. FRANKLIN SI STR. EPISCOPIEI", 5m, 72 },
                    { 165, 35, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(4784), "CLOPOTARII VECHI", "INTRE STR. GRIGORE ALEXANDRESCU SI BD. IANCU DE HUNEDOARA", 5m, 35 },
                    { 198, 152, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(6430), "FRUMOASA", "INTRE CALEA VICTORIEI SI STR. SEVASTOPOL", 5m, 152 },
                    { 199, 119, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(6477), "GARA BANEASA", "ESPLANADA GARII BANEASA SI STR. TIPOGRAFILOR", 5m, 119 },
                    { 200, 13, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(6551), "GARA CEAS", "PIATA GARII DE NORD", 5m, 13 },
                    { 201, 39, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(6600), "GARA PARC", "PIATA GARII DE NORD", 5m, 39 },
                    { 202, 25, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(6692), "GARII DE NORD 1", "STR. GARII DE NORD, INTRE STR. BERZEI SI STR. CAMELIEI", 2.5m, 25 },
                    { 203, 77, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(6739), "GARII DE NORD 2", "STR. GARII DE NORD, INTRE STR. CAMELIEI SI PTA. GARII DE NORD", 2.5m, 77 },
                    { 204, 147, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(6780), "GEORGE CALINESCU", "", 2.5m, 147 },
                    { 205, 35, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(6823), "GEORGE ENESCU", "STR. GEORGE ENESCU, INTRE STR. MENDELEEV SI BD. MAGHERU", 5m, 35 },
                    { 206, 65, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(6861), "GEORGE ENESCU 3", "INTRE STR. D.I.MENDELEEV SI CALEA VICTORIEI", 5m, 65 },
                    { 207, 54, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(6918), "GHEORGHE DUCA", "INTRE CALEA GRIVITEI SI A.I.CUZA", 2.5m, 54 },
                    { 208, 46, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(6985), "GHEORGHE LAZAR", "INTRE SF. CONSTANTIN SI GH.COBALCESCU", 2.5m, 46 },
                    { 209, 31, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(7028), "GRADINA CU CAI", "INTRE SP. INDEPENDENTEI SI PTA. M.KOGALNICEANU", 5m, 31 },
                    { 210, 30, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(7068), "GRADINITA – ROSETTI", "STR. C.A.ROSETTI, INTRE STR. T. ARGHEZI SI STR. DAVID PRAPORGESCU", 10m, 30 },
                    { 197, 93, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(6364), "FICUSULUI 2", "INTRE STR. H.MACELARIU SI BD. AEROGARII", 2.5m, 93 },
                    { 211, 41, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(7109), "GRIGORE ALEXANDRESCU", "STR. GRIGORE ALEXANDRESCU, INTRE CALEA VICTORIEI SI BD. LASCAR CATARGIU", 5m, 41 },
                    { 213, 118, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(7190), "GRIGORE COBALCESCU", "INTRE BD. SCHITU MAGUREANU SI STR. BERZEI", 5m, 118 },
                    { 214, 82, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(7228), "GUTENBERG", "", 2.5m, 82 },
                    { 215, 15, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(7268), "HALELOR", "STR. HALELOR, INTRE STR. SELARI SI STR. CALDARARI", 10m, 15 },
                    { 216, 99, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(7306), "HERASTRAU 1 (PAVILION H)", "PARC HERASTRAU", 5m, 99 }
                });

            migrationBuilder.InsertData(
                table: "ParkingAreas",
                columns: new[] { "Id", "AvailableSpots", "City", "CreatedOn", "Emplacement", "EmplacementLocation", "PricePerHour", "TotalSpots" },
                values: new object[,]
                {
                    { 217, 132, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(7345), "HERASTRAU 2 BOA", "PARC MIORITA (HERASTRAU)", 5m, 132 },
                    { 218, 14, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(7437), "HOROSCOP 1", "BD. D. CANTEMIR - INTRE SPLAIUL UNIRII SI STR. BIBESCU VODA", 5m, 14 },
                    { 219, 11, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(7511), "HOROSCOP 2 SITRACO", "BD. D. CANTEMIR - INTRE SPLAIUL UNIRII SI STR. BIBESCU VODA", 5m, 11 },
                    { 220, 24, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(7563), "HOROSCOP 3 SPLAI", "BD. D. CANTEMIR - SPLAIUL UNIRII", 5m, 24 },
                    { 221, 20, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(7603), "HRISTO BOTEV 1", "INTRE STR. SCAUNE SI STR. IVO ANDRIC", 5m, 20 },
                    { 222, 25, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(7644), "HRISTO BOTEV 2", "INTRE STR. NEGUSTORI SI STR. TEODOR STEFANESCU", 5m, 25 },
                    { 223, 91, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(7683), "IACOB FELIX 1", "INTRE STR. GHE. POLIZU SI SOS. N. TITULESCU", 2.5m, 91 },
                    { 224, 33, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(7723), "IACOB FELIX 2", "INTRE SOS. N. TITULESCU SI BD. I. MIHALACHE", 2.5m, 33 },
                    { 225, 36, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(7763), "IANCU DE HUNEDOARA 1", "INTRE BD. LASCAR CATARGIU SI INTR. GENEVA", 5m, 36 },
                    { 212, 216, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(7150), "GRIGORE ALEXANDRESCU 2", "INTRE BD. LASCAR CATARGIU SI CALEA DOROBANTI", 5m, 216 },
                    { 102, 8, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(1181), "ALEEA CAUZASI", "INTERSECTIA CU BD. MIRCEA VODA", 5m, 8 },
                    { 196, 46, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(6308), "FICUSULUI 1", "INTRE STR. E.VACARESCU SI STR. H.MACELARIU", 2.5m, 46 },
                    { 194, 73, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(6220), "EVA", "BD. GHE. MAGHERU, INTRE STR. G.ENESCU SI STR. ANASTASIE SIMU", 5m, 73 },
                    { 166, 54, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(4825), "CONSERVATOR – UNESCO", "STR. STIRBEI VODA", 5m, 54 },
                    { 167, 84, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(4866), "CONSTANTIN MILLE", "INTRE CALEA VICTORIEI SI STR. I. BREZOIANU", 5m, 84 },
                    { 168, 33, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(4906), "CONSTANTIN NACU", "INTRE PTA. ITALIANA SI BD. CAROL I", 5m, 33 },
                    { 169, 92, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(4945), "CRANGASI", "PTA. CRANGASI", 2.5m, 92 },
                    { 170, 36, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(5072), "CRETULESCU", "ALEEA DE LANGA BISERICA CRETULESCU", 5m, 36 },
                    { 171, 40, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(5121), "DACIA 2", "BD. DACIA", 5m, 40 },
                    { 172, 21, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(5162), "DACIA MONTEORU", "BD. DACIA X CALEA VICTORIEI", 5m, 21 },
                    { 173, 69, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(5200), "DANIELOPOL", "INTRE SPLAIUL INDEPENDENTEI SI STR. POENARU BORDEA", 5m, 69 },
                    { 174, 64, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(5250), "DECEBAL 1", "CALEA MOSILOR (PTA. DECEBAL)", 10m, 64 },
                    { 175, 44, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(5317), "DECEBAL 2", "CALEA MOSILOR (PTA. DECEBAL)", 10m, 44 },
                    { 176, 53, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(5368), "DECEBAL 3", "CALEA MOSILOR (PTA. DECEBAL)", 10m, 53 },
                    { 177, 67, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(5408), "DIANEI", "INTRE ITALIANA SI CAROL I", 5m, 67 },
                    { 178, 72, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(5448), "DINICU GOLESCU (GARA)", "", 2.5m, 72 },
                    { 195, 188, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(6262), "EXPOZITIEI", "BD. EXPOZITIEI (ZONA ROMEXPO)", 5m, 188 },
                    { 179, 111, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(5489), "DINICU GOLESCU 1", "INTRE STR. M. VULCANESCU SI PTA. GARII DE NORD", 2.5m, 111 },
                    { 181, 18, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(5571), "DOAMNEI 1", "STR. DOAMNEI, INTRE BD. I.C.BRATIANU SI INTR. SELARI", 10m, 18 },
                    { 182, 21, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(5615), "DOAMNEI 2", "", 10m, 21 },
                    { 183, 28, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(5655), "DOAMNEI 4", "", 10m, 28 },
                    { 184, 57, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(5696), "DOMNITA ANASTASIA", "INTRE STR. LIPSCANI SI STR. A. SALIGNY", 5m, 57 },
                    { 185, 21, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(5735), "DOROBANTI", "PTA. DOROBANTI", 5m, 21 },
                    { 186, 11, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(5840), "DOROBANTI – LAHOVARI", "", 5m, 11 },
                    { 187, 11, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(5911), "DOROBANTI DACIA", "INTERSECTIE BD. DACIA SI CALEA DOROBANTILOR", 5m, 11 },
                    { 188, 19, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(5958), "DRISTOR 1", "INTRE C.RESSU SI STATIE METROU", 2.5m, 19 },
                    { 189, 21, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(6002), "DRISTOR 2", "INTRE STATIE METROU SI GRAULUI", 2.5m, 21 },
                    { 190, 107, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(6046), "EDGAR QUINET", "INTRE CALEA VICTORIEI SI BISERICA ENEI", 10m, 107 },
                    { 191, 48, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(6090), "EFORIE", "INTRE C.VICTORIEI SI STR. D. ANASTASIA", 5m, 48 },
                    { 192, 64, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(6133), "ELENA VACARESCU", "INTRE BD. FICUSULUI SI SOS. BUCURESTI-PLOIESTI", 2.5m, 64 },
                    { 193, 77, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(6177), "EPISCOPIEI", "INTRE N.GOLESCU SI C. VICTORIEI", 5m, 77 },
                    { 180, 120, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(5531), "DIONISIE LUPU", "INTRE STR. G-RAL EREMIA GRIGORESCU SI C.A.ROSETTI", 5m, 120 }
                });

            migrationBuilder.InsertData(
                table: "ParkingAreas",
                columns: new[] { "Id", "AvailableSpots", "City", "CreatedOn", "Emplacement", "EmplacementLocation", "PricePerHour", "TotalSpots" },
                values: new object[] { 101, 29, "București", new DateTime(2022, 2, 1, 12, 26, 31, 888, DateTimeKind.Local).AddTicks(935), "Alee Legatura Banu Antonache si Calea Floreasca", "", 2.5m, 29 });

            migrationBuilder.InsertData(
                table: "User",
                columns: new[] { "Id", "CreatedOn", "Email", "Name", "Password", "Phone", "Role" },
                values: new object[] { 1, new DateTime(2022, 2, 1, 12, 26, 31, 360, DateTimeKind.Local).AddTicks(516), "administrator220@easypark.com", "ADMINISTRATOR", "$2a$11$uL.2P36knxZ9gewRybZUoefpWrcwHBlbxN4tP9fX3ZyTF9i9gmCoe", "0723678956", 220 });

            migrationBuilder.InsertData(
                table: "User",
                columns: new[] { "Id", "CreatedOn", "Email", "Name", "Password", "Phone", "Role" },
                values: new object[] { 3, new DateTime(2022, 2, 1, 12, 26, 31, 557, DateTimeKind.Local).AddTicks(9751), "admin_area342@easypark.com", "ADMIN_AREA342", "$2a$11$E9KNRv6eF3SWotZ3ucCrH.bv6J4DZWIuxt9baSeYaOX3c4cCvr9N6", "0734670055", 210 });

            migrationBuilder.InsertData(
                table: "Admins",
                columns: new[] { "Id", "CreatedOn", "ParkingAreaId", "UserId" },
                values: new object[] { 1, new DateTime(2022, 2, 1, 12, 26, 31, 747, DateTimeKind.Local).AddTicks(7720), 342, 3 });

            migrationBuilder.CreateIndex(
                name: "IX_Admins_ParkingAreaId",
                table: "Admins",
                column: "ParkingAreaId",
                unique: true,
                filter: "[ParkingAreaId] IS NOT NULL");

            migrationBuilder.CreateIndex(
                name: "IX_Admins_UserId",
                table: "Admins",
                column: "UserId");

            migrationBuilder.CreateIndex(
                name: "IX_Drivers_LicenseId",
                table: "Drivers",
                column: "LicenseId",
                unique: true,
                filter: "[LicenseId] IS NOT NULL");

            migrationBuilder.CreateIndex(
                name: "IX_Drivers_UserId",
                table: "Drivers",
                column: "UserId");

            migrationBuilder.CreateIndex(
                name: "IX_Reservations_ParkingAreaId",
                table: "Reservations",
                column: "ParkingAreaId");

            migrationBuilder.CreateIndex(
                name: "IX_Reservations_VehicleId",
                table: "Reservations",
                column: "VehicleId");

            migrationBuilder.CreateIndex(
                name: "IX_Vehicles_DriverId",
                table: "Vehicles",
                column: "DriverId");
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "Admins");

            migrationBuilder.DropTable(
                name: "Reservations");

            migrationBuilder.DropTable(
                name: "ParkingAreas");

            migrationBuilder.DropTable(
                name: "Vehicles");

            migrationBuilder.DropTable(
                name: "Drivers");

            migrationBuilder.DropTable(
                name: "DrivingLicense");

            migrationBuilder.DropTable(
                name: "User");
        }
    }
}