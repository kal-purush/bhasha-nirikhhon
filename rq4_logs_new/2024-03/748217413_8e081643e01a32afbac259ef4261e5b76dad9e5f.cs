using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace Map.EFCore.Migrations
{
    /// <inheritdoc />
    public partial class UpdateDbSchemaAndIdTypes : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "Steps",
                columns: table => new
                {
                    StepId = table.Column<int>(type: "int", nullable: false)
                        .Annotation("SqlServer:Identity", "1, 1"),
                    TripId = table.Column<Guid>(type: "uniqueidentifier", nullable: false),
                    Name = table.Column<string>(type: "nvarchar(max)", nullable: false),
                    Latitude = table.Column<decimal>(type: "decimal(18,12)", precision: 18, scale: 12, nullable: false),
                    Longitude = table.Column<decimal>(type: "decimal(18,12)", precision: 18, scale: 12, nullable: false),
                    Description = table.Column<string>(type: "nvarchar(500)", maxLength: 500, nullable: true),
                    StartDate = table.Column<DateTime>(type: "datetime2", nullable: true),
                    EndDate = table.Column<DateTime>(type: "datetime2", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Steps", x => x.StepId);
                    table.ForeignKey(
                        name: "FK_Steps_Trips_TripId",
                        column: x => x.TripId,
                        principalTable: "Trips",
                        principalColumn: "TripId",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "TravelRoads",
                columns: table => new
                {
                    TravelId = table.Column<int>(type: "int", nullable: false)
                        .Annotation("SqlServer:Identity", "1, 1"),
                    RoadCoordinates = table.Column<string>(type: "nvarchar(max)", maxLength: -1, nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_TravelRoads", x => x.TravelId);
                });

            migrationBuilder.CreateTable(
                name: "Travels",
                columns: table => new
                {
                    TravelId = table.Column<int>(type: "int", nullable: false),
                    OriginStepId = table.Column<int>(type: "int", nullable: false),
                    DestinationStepId = table.Column<int>(type: "int", nullable: false),
                    TransportMode = table.Column<string>(type: "nvarchar(max)", nullable: false),
                    Distance = table.Column<decimal>(type: "decimal(18,12)", precision: 18, scale: 12, nullable: false),
                    Duration = table.Column<decimal>(type: "decimal(18,12)", precision: 18, scale: 12, nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Travels", x => x.TravelId);
                    table.ForeignKey(
                        name: "FK_Travels_Steps_DestinationStepId",
                        column: x => x.DestinationStepId,
                        principalTable: "Steps",
                        principalColumn: "StepId",
                        onDelete: ReferentialAction.Restrict);
                    table.ForeignKey(
                        name: "FK_Travels_Steps_OriginStepId",
                        column: x => x.OriginStepId,
                        principalTable: "Steps",
                        principalColumn: "StepId",
                        onDelete: ReferentialAction.Restrict);
                    table.ForeignKey(
                        name: "FK_Travels_TravelRoads_TravelId",
                        column: x => x.TravelId,
                        principalTable: "TravelRoads",
                        principalColumn: "TravelId",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateIndex(
                name: "IX_Steps_TripId",
                table: "Steps",
                column: "TripId");

            migrationBuilder.CreateIndex(
                name: "IX_Travels_DestinationStepId",
                table: "Travels",
                column: "DestinationStepId",
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_Travels_OriginStepId",
                table: "Travels",
                column: "OriginStepId",
                unique: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "Travels");

            migrationBuilder.DropTable(
                name: "Steps");

            migrationBuilder.DropTable(
                name: "TravelRoads");
        }
    }
}