using System;
using Microsoft.EntityFrameworkCore.Migrations;

namespace CM.Data.Migrations
{
    public partial class Initial : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "AspNetRoles",
                columns: table => new
                {
                    Id = table.Column<Guid>(nullable: false),
                    Name = table.Column<string>(maxLength: 256, nullable: true),
                    NormalizedName = table.Column<string>(maxLength: 256, nullable: true),
                    ConcurrencyStamp = table.Column<string>(nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_AspNetRoles", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "AspNetUsers",
                columns: table => new
                {
                    Id = table.Column<Guid>(nullable: false),
                    UserName = table.Column<string>(maxLength: 256, nullable: true),
                    NormalizedUserName = table.Column<string>(maxLength: 256, nullable: true),
                    Email = table.Column<string>(maxLength: 256, nullable: true),
                    NormalizedEmail = table.Column<string>(maxLength: 256, nullable: true),
                    EmailConfirmed = table.Column<bool>(nullable: false),
                    PasswordHash = table.Column<string>(nullable: true),
                    SecurityStamp = table.Column<string>(nullable: true),
                    ConcurrencyStamp = table.Column<string>(nullable: true),
                    PhoneNumber = table.Column<string>(nullable: true),
                    PhoneNumberConfirmed = table.Column<bool>(nullable: false),
                    TwoFactorEnabled = table.Column<bool>(nullable: false),
                    LockoutEnd = table.Column<DateTimeOffset>(nullable: true),
                    LockoutEnabled = table.Column<bool>(nullable: false),
                    AccessFailedCount = table.Column<int>(nullable: false),
                    DeletedOn = table.Column<DateTime>(nullable: true),
                    IsDeleted = table.Column<bool>(nullable: false),
                    ImagePath = table.Column<string>(nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_AspNetUsers", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "Bars",
                columns: table => new
                {
                    Id = table.Column<Guid>(nullable: false),
                    Name = table.Column<string>(maxLength: 256, nullable: false),
                    AddressID = table.Column<Guid>(nullable: false),
                    Phone = table.Column<string>(nullable: true),
                    ImagePath = table.Column<string>(nullable: true),
                    Details = table.Column<string>(nullable: true),
                    IsUnlisted = table.Column<bool>(nullable: false),
                    AverageRating = table.Column<double>(nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Bars", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "Cocktails",
                columns: table => new
                {
                    Id = table.Column<Guid>(nullable: false),
                    Name = table.Column<string>(maxLength: 256, nullable: false),
                    Recipe = table.Column<string>(nullable: true),
                    ImagePath = table.Column<string>(nullable: true),
                    IsUnlisted = table.Column<bool>(nullable: false),
                    AverageRating = table.Column<double>(nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Cocktails", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "Countries",
                columns: table => new
                {
                    Id = table.Column<Guid>(nullable: false),
                    Name = table.Column<string>(nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Countries", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "Ingredients",
                columns: table => new
                {
                    Id = table.Column<Guid>(nullable: false),
                    Name = table.Column<string>(nullable: true),
                    ImagePath = table.Column<string>(nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Ingredients", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "AspNetRoleClaims",
                columns: table => new
                {
                    Id = table.Column<int>(nullable: false)
                        .Annotation("SqlServer:Identity", "1, 1"),
                    RoleId = table.Column<Guid>(nullable: false),
                    ClaimType = table.Column<string>(nullable: true),
                    ClaimValue = table.Column<string>(nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_AspNetRoleClaims", x => x.Id);
                    table.ForeignKey(
                        name: "FK_AspNetRoleClaims_AspNetRoles_RoleId",
                        column: x => x.RoleId,
                        principalTable: "AspNetRoles",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "AspNetUserClaims",
                columns: table => new
                {
                    Id = table.Column<int>(nullable: false)
                        .Annotation("SqlServer:Identity", "1, 1"),
                    UserId = table.Column<Guid>(nullable: false),
                    ClaimType = table.Column<string>(nullable: true),
                    ClaimValue = table.Column<string>(nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_AspNetUserClaims", x => x.Id);
                    table.ForeignKey(
                        name: "FK_AspNetUserClaims_AspNetUsers_UserId",
                        column: x => x.UserId,
                        principalTable: "AspNetUsers",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "AspNetUserLogins",
                columns: table => new
                {
                    LoginProvider = table.Column<string>(nullable: false),
                    ProviderKey = table.Column<string>(nullable: false),
                    ProviderDisplayName = table.Column<string>(nullable: true),
                    UserId = table.Column<Guid>(nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_AspNetUserLogins", x => new { x.LoginProvider, x.ProviderKey });
                    table.ForeignKey(
                        name: "FK_AspNetUserLogins_AspNetUsers_UserId",
                        column: x => x.UserId,
                        principalTable: "AspNetUsers",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "AspNetUserRoles",
                columns: table => new
                {
                    UserId = table.Column<Guid>(nullable: false),
                    RoleId = table.Column<Guid>(nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_AspNetUserRoles", x => new { x.UserId, x.RoleId });
                    table.ForeignKey(
                        name: "FK_AspNetUserRoles_AspNetRoles_RoleId",
                        column: x => x.RoleId,
                        principalTable: "AspNetRoles",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_AspNetUserRoles_AspNetUsers_UserId",
                        column: x => x.UserId,
                        principalTable: "AspNetUsers",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "AspNetUserTokens",
                columns: table => new
                {
                    UserId = table.Column<Guid>(nullable: false),
                    LoginProvider = table.Column<string>(nullable: false),
                    Name = table.Column<string>(nullable: false),
                    Value = table.Column<string>(nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_AspNetUserTokens", x => new { x.UserId, x.LoginProvider, x.Name });
                    table.ForeignKey(
                        name: "FK_AspNetUserTokens_AspNetUsers_UserId",
                        column: x => x.UserId,
                        principalTable: "AspNetUsers",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "BarComments",
                columns: table => new
                {
                    Id = table.Column<Guid>(nullable: false),
                    AppUserId = table.Column<Guid>(nullable: false),
                    Text = table.Column<string>(maxLength: 500, nullable: false),
                    CommentedOn = table.Column<DateTimeOffset>(nullable: false),
                    BarId = table.Column<Guid>(nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_BarComments", x => x.Id);
                    table.ForeignKey(
                        name: "FK_BarComments_AspNetUsers_AppUserId",
                        column: x => x.AppUserId,
                        principalTable: "AspNetUsers",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Restrict);
                    table.ForeignKey(
                        name: "FK_BarComments_Bars_BarId",
                        column: x => x.BarId,
                        principalTable: "Bars",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Restrict);
                });

            migrationBuilder.CreateTable(
                name: "BarRatings",
                columns: table => new
                {
                    AppUserId = table.Column<Guid>(nullable: false),
                    BarId = table.Column<Guid>(nullable: false),
                    Score = table.Column<int>(nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_BarRatings", x => new { x.BarId, x.AppUserId });
                    table.ForeignKey(
                        name: "FK_BarRatings_AspNetUsers_AppUserId",
                        column: x => x.AppUserId,
                        principalTable: "AspNetUsers",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Restrict);
                    table.ForeignKey(
                        name: "FK_BarRatings_Bars_BarId",
                        column: x => x.BarId,
                        principalTable: "Bars",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Restrict);
                });

            migrationBuilder.CreateTable(
                name: "BarCocktails",
                columns: table => new
                {
                    BarId = table.Column<Guid>(nullable: false),
                    CocktailId = table.Column<Guid>(nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_BarCocktails", x => new { x.CocktailId, x.BarId });
                    table.ForeignKey(
                        name: "FK_BarCocktails_Bars_BarId",
                        column: x => x.BarId,
                        principalTable: "Bars",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Restrict);
                    table.ForeignKey(
                        name: "FK_BarCocktails_Cocktails_CocktailId",
                        column: x => x.CocktailId,
                        principalTable: "Cocktails",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Restrict);
                });

            migrationBuilder.CreateTable(
                name: "CocktailComments",
                columns: table => new
                {
                    Id = table.Column<Guid>(nullable: false),
                    AppUserId = table.Column<Guid>(nullable: false),
                    Text = table.Column<string>(maxLength: 500, nullable: false),
                    CommentedOn = table.Column<DateTimeOffset>(nullable: false),
                    CocktailId = table.Column<Guid>(nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_CocktailComments", x => x.Id);
                    table.ForeignKey(
                        name: "FK_CocktailComments_AspNetUsers_AppUserId",
                        column: x => x.AppUserId,
                        principalTable: "AspNetUsers",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Restrict);
                    table.ForeignKey(
                        name: "FK_CocktailComments_Cocktails_CocktailId",
                        column: x => x.CocktailId,
                        principalTable: "Cocktails",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Restrict);
                });

            migrationBuilder.CreateTable(
                name: "CocktailRatings",
                columns: table => new
                {
                    AppUserId = table.Column<Guid>(nullable: false),
                    CocktailId = table.Column<Guid>(nullable: false),
                    Score = table.Column<int>(nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_CocktailRatings", x => new { x.CocktailId, x.AppUserId });
                    table.ForeignKey(
                        name: "FK_CocktailRatings_AspNetUsers_AppUserId",
                        column: x => x.AppUserId,
                        principalTable: "AspNetUsers",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Restrict);
                    table.ForeignKey(
                        name: "FK_CocktailRatings_Cocktails_CocktailId",
                        column: x => x.CocktailId,
                        principalTable: "Cocktails",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Restrict);
                });

            migrationBuilder.CreateTable(
                name: "Cities",
                columns: table => new
                {
                    Id = table.Column<Guid>(nullable: false),
                    Name = table.Column<string>(nullable: true),
                    CountryId = table.Column<Guid>(nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Cities", x => x.Id);
                    table.ForeignKey(
                        name: "FK_Cities_Countries_CountryId",
                        column: x => x.CountryId,
                        principalTable: "Countries",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "CocktailIngredients",
                columns: table => new
                {
                    CocktailId = table.Column<Guid>(nullable: false),
                    IngredientId = table.Column<Guid>(nullable: false),
                    Ammount = table.Column<int>(nullable: false),
                    Unit = table.Column<string>(nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_CocktailIngredients", x => new { x.CocktailId, x.IngredientId });
                    table.ForeignKey(
                        name: "FK_CocktailIngredients_Cocktails_CocktailId",
                        column: x => x.CocktailId,
                        principalTable: "Cocktails",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Restrict);
                    table.ForeignKey(
                        name: "FK_CocktailIngredients_Ingredients_IngredientId",
                        column: x => x.IngredientId,
                        principalTable: "Ingredients",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Restrict);
                });

            migrationBuilder.CreateTable(
                name: "Addresses",
                columns: table => new
                {
                    Id = table.Column<Guid>(nullable: false),
                    CityId = table.Column<Guid>(nullable: false),
                    Street = table.Column<string>(nullable: true),
                    BarId = table.Column<Guid>(nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Addresses", x => x.Id);
                    table.ForeignKey(
                        name: "FK_Addresses_Bars_BarId",
                        column: x => x.BarId,
                        principalTable: "Bars",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Restrict);
                    table.ForeignKey(
                        name: "FK_Addresses_Cities_CityId",
                        column: x => x.CityId,
                        principalTable: "Cities",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.InsertData(
                table: "AspNetRoles",
                columns: new[] { "Id", "ConcurrencyStamp", "Name", "NormalizedName" },
                values: new object[,]
                {
                    { new Guid("a6dc0db8-408c-4aff-bf99-0d46efd31787"), "fd52d82b-ef8e-49e0-8245-9e3b68525b01", "Admin", "ADMIN" },
                    { new Guid("acde9ca2-de8c-45a0-ad81-3c3b05c8c90e"), "46bdd707-11ef-40bd-9a17-45d75520b0c3", "Member", "MEMBER" }
                });

            migrationBuilder.InsertData(
                table: "AspNetUsers",
                columns: new[] { "Id", "AccessFailedCount", "ConcurrencyStamp", "DeletedOn", "Email", "EmailConfirmed", "ImagePath", "IsDeleted", "LockoutEnabled", "LockoutEnd", "NormalizedEmail", "NormalizedUserName", "PasswordHash", "PhoneNumber", "PhoneNumberConfirmed", "SecurityStamp", "TwoFactorEnabled", "UserName" },
                values: new object[,]
                {
                    { new Guid("98190af6-ba8e-44ff-8619-4d3b90040b5b"), 0, "220e6a07-b96e-4d54-b4dd-118d21773df8", null, null, false, null, false, false, null, null, null, null, null, false, null, false, "testUser@test.com" },
                    { new Guid("e355f8c4-ee01-4986-89bb-d1b56d17ae23"), 0, "e74bcf15-a932-491e-9c30-7e0dff375b63", null, null, false, null, false, false, null, null, null, null, null, false, null, false, "testUser1@test.com" }
                });

            migrationBuilder.InsertData(
                table: "Bars",
                columns: new[] { "Id", "AddressID", "AverageRating", "Details", "ImagePath", "IsUnlisted", "Name", "Phone" },
                values: new object[,]
                {
                    { new Guid("9bdbf5e7-ad83-415c-b359-9ff5e2f0dedd"), new Guid("73f2c4c2-78ae-45ab-b82c-b06a48271a6d"), null, "Weaving tradition with modernity, there’s something heart-warming about the story of Dante. When Linden Pride, Nathalie Hudson and Naren Young took over this Greenwich Village site 100 years after it first opened they could see the things that made this fading Italian café once great could be relevant again. At the heart of their mission was to renew the bar, while being authentic to its roots and appealing to the Greenwich Village community. So the classical décor was given a lift, and in came refined but wholesome Italian food, aperitivos and cocktails. There is a whole list of Negronis to make your way through, but that’s OK because Dante is an all-day restaurant-bar. The Garibaldi too is a must-order. Made with Campari and ‘fluffy’ orange juice, it has brought this once-dusty drink back to life. The measure of a bar is the experience of its customers – in hospitality, drinks and food Dante has the fundamentals down to a fine art, earning the deserved title of The World's Best Bar 2019, sponsored by Perrier.", "/images/9BDBF5E7-AD83-415C-B359-9FF5E2F0DEDD.jpg", false, "Dante", "(212) 982-5275" },
                    { new Guid("0899e918-977c-44d5-a5cb-de9559ad822c"), new Guid("4ecac9dc-31df-4b89-93b5-5550d2608ede"), null, "No matter the workings of the cocktail world around it, the Connaught Bar stays true to its principles – artful drinks and graceful service in a stylish setting. Under the watchful gaze of Ago Perrone, the hotel’s director of mixology, the bar moves forward with an effortless glide. Last year marked 10 years since designer David Collins unveiled the bar’s elegant Cubist interior and in celebration it launched its own gin, crafted in the building by none other than Perrone himself. It’s already the most called-for spirit on the showpiece trolley that clinks between the bar’s discerning guests, serving personalised Martinis. The latest cocktail menu, Vanguard, has upped the invention – Number 11 is an embellished Vesper served in Martini glasses hand-painted every day in house, while the Gate No.1 is a luscious blend of spirits, wines and jam. But of course, the Connaught Masterpieces isn’t a chapter easily overlooked. Along with the Dry Martini, the Bloody Mary is liquid perfection and the Mulatta Daisy is Perrone’s own classic, in and out of the bar. In 2019, Connaught Bar earns the title of The Best Bar in Europe, sponsored by Michter's.", "/images/0899E918-977C-44D5-A5CB-DE9559AD822C-logo.png", false, "Connaught Bar", "+44 (0)20 7499 7070" }
                });

            migrationBuilder.InsertData(
                table: "Cocktails",
                columns: new[] { "Id", "AverageRating", "ImagePath", "IsUnlisted", "Name", "Recipe" },
                values: new object[,]
                {
                    { new Guid("a3fd2a00-52c4-4293-a184-6f448d008015"), null, null, false, "Loch Lomond", null },
                    { new Guid("347e304b-03cd-414f-91b2-faed4fdb86e9"), null, null, false, "Strawberry Lemonade", null },
                    { new Guid("3f088822-fa2c-45f1-aa96-067f07aa04ea"), null, null, false, "Rum Milk Punch", null }
                });

            migrationBuilder.InsertData(
                table: "Countries",
                columns: new[] { "Id", "Name" },
                values: new object[,]
                {
                    { new Guid("4828b9db-cd3a-487f-9782-7a23653ff99a"), "USA" },
                    { new Guid("4ecac9dc-31df-4b89-93b5-5550d2608ede"), "UK" }
                });

            migrationBuilder.InsertData(
                table: "BarCocktails",
                columns: new[] { "CocktailId", "BarId" },
                values: new object[,]
                {
                    { new Guid("a3fd2a00-52c4-4293-a184-6f448d008015"), new Guid("9bdbf5e7-ad83-415c-b359-9ff5e2f0dedd") },
                    { new Guid("347e304b-03cd-414f-91b2-faed4fdb86e9"), new Guid("9bdbf5e7-ad83-415c-b359-9ff5e2f0dedd") },
                    { new Guid("3f088822-fa2c-45f1-aa96-067f07aa04ea"), new Guid("0899e918-977c-44d5-a5cb-de9559ad822c") }
                });

            migrationBuilder.InsertData(
                table: "BarRatings",
                columns: new[] { "BarId", "AppUserId", "Score" },
                values: new object[,]
                {
                    { new Guid("9bdbf5e7-ad83-415c-b359-9ff5e2f0dedd"), new Guid("98190af6-ba8e-44ff-8619-4d3b90040b5b"), 5 },
                    { new Guid("9bdbf5e7-ad83-415c-b359-9ff5e2f0dedd"), new Guid("e355f8c4-ee01-4986-89bb-d1b56d17ae23"), 1 }
                });

            migrationBuilder.InsertData(
                table: "Cities",
                columns: new[] { "Id", "CountryId", "Name" },
                values: new object[,]
                {
                    { new Guid("320b050b-82f1-494c-9add-91ab28bf98dd"), new Guid("4828b9db-cd3a-487f-9782-7a23653ff99a"), "New York" },
                    { new Guid("0eea5eb5-6151-41be-ac73-b35e056ca97e"), new Guid("4ecac9dc-31df-4b89-93b5-5550d2608ede"), "London" }
                });

            migrationBuilder.InsertData(
                table: "Addresses",
                columns: new[] { "Id", "BarId", "CityId", "Street" },
                values: new object[] { new Guid("73f2c4c2-78ae-45ab-b82c-b06a48271a6d"), new Guid("9bdbf5e7-ad83-415c-b359-9ff5e2f0dedd"), new Guid("320b050b-82f1-494c-9add-91ab28bf98dd"), "79-81 MACDOUGAL ST" });

            migrationBuilder.InsertData(
                table: "Addresses",
                columns: new[] { "Id", "BarId", "CityId", "Street" },
                values: new object[] { new Guid("dfaa43d9-d6d8-4a15-bda9-48823fa4b886"), new Guid("0899e918-977c-44d5-a5cb-de9559ad822c"), new Guid("0eea5eb5-6151-41be-ac73-b35e056ca97e"), "Mayfair W1K 2AL" });

            migrationBuilder.CreateIndex(
                name: "IX_Addresses_BarId",
                table: "Addresses",
                column: "BarId",
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_Addresses_CityId",
                table: "Addresses",
                column: "CityId");

            migrationBuilder.CreateIndex(
                name: "IX_AspNetRoleClaims_RoleId",
                table: "AspNetRoleClaims",
                column: "RoleId");

            migrationBuilder.CreateIndex(
                name: "RoleNameIndex",
                table: "AspNetRoles",
                column: "NormalizedName",
                unique: true,
                filter: "[NormalizedName] IS NOT NULL");

            migrationBuilder.CreateIndex(
                name: "IX_AspNetUserClaims_UserId",
                table: "AspNetUserClaims",
                column: "UserId");

            migrationBuilder.CreateIndex(
                name: "IX_AspNetUserLogins_UserId",
                table: "AspNetUserLogins",
                column: "UserId");

            migrationBuilder.CreateIndex(
                name: "IX_AspNetUserRoles_RoleId",
                table: "AspNetUserRoles",
                column: "RoleId");

            migrationBuilder.CreateIndex(
                name: "EmailIndex",
                table: "AspNetUsers",
                column: "NormalizedEmail");

            migrationBuilder.CreateIndex(
                name: "UserNameIndex",
                table: "AspNetUsers",
                column: "NormalizedUserName",
                unique: true,
                filter: "[NormalizedUserName] IS NOT NULL");

            migrationBuilder.CreateIndex(
                name: "IX_BarCocktails_BarId",
                table: "BarCocktails",
                column: "BarId");

            migrationBuilder.CreateIndex(
                name: "IX_BarComments_AppUserId",
                table: "BarComments",
                column: "AppUserId");

            migrationBuilder.CreateIndex(
                name: "IX_BarComments_BarId",
                table: "BarComments",
                column: "BarId");

            migrationBuilder.CreateIndex(
                name: "IX_BarRatings_AppUserId",
                table: "BarRatings",
                column: "AppUserId");

            migrationBuilder.CreateIndex(
                name: "IX_Cities_CountryId",
                table: "Cities",
                column: "CountryId");

            migrationBuilder.CreateIndex(
                name: "IX_CocktailComments_AppUserId",
                table: "CocktailComments",
                column: "AppUserId");

            migrationBuilder.CreateIndex(
                name: "IX_CocktailComments_CocktailId",
                table: "CocktailComments",
                column: "CocktailId");

            migrationBuilder.CreateIndex(
                name: "IX_CocktailIngredients_IngredientId",
                table: "CocktailIngredients",
                column: "IngredientId");

            migrationBuilder.CreateIndex(
                name: "IX_CocktailRatings_AppUserId",
                table: "CocktailRatings",
                column: "AppUserId");
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "Addresses");

            migrationBuilder.DropTable(
                name: "AspNetRoleClaims");

            migrationBuilder.DropTable(
                name: "AspNetUserClaims");

            migrationBuilder.DropTable(
                name: "AspNetUserLogins");

            migrationBuilder.DropTable(
                name: "AspNetUserRoles");

            migrationBuilder.DropTable(
                name: "AspNetUserTokens");

            migrationBuilder.DropTable(
                name: "BarCocktails");

            migrationBuilder.DropTable(
                name: "BarComments");

            migrationBuilder.DropTable(
                name: "BarRatings");

            migrationBuilder.DropTable(
                name: "CocktailComments");

            migrationBuilder.DropTable(
                name: "CocktailIngredients");

            migrationBuilder.DropTable(
                name: "CocktailRatings");

            migrationBuilder.DropTable(
                name: "Cities");

            migrationBuilder.DropTable(
                name: "AspNetRoles");

            migrationBuilder.DropTable(
                name: "Bars");

            migrationBuilder.DropTable(
                name: "Ingredients");

            migrationBuilder.DropTable(
                name: "AspNetUsers");

            migrationBuilder.DropTable(
                name: "Cocktails");

            migrationBuilder.DropTable(
                name: "Countries");
        }
    }
}