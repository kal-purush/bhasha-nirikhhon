using System;
using Microsoft.EntityFrameworkCore.Migrations;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata;

namespace API_Asada.Migrations
{
    public partial class DB_Migrations : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "Aforo",
                columns: table => new
                {
                    Id = table.Column<int>(nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.SerialColumn),
                    fecha = table.Column<DateTime>(nullable: false),
                    tiempo = table.Column<DateTime>(nullable: false),
                    litros = table.Column<double>(nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Aforo", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "Agua_No_Contabilizada",
                columns: table => new
                {
                    Id = table.Column<int>(nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.SerialColumn),
                    sector = table.Column<string>(maxLength: 2, nullable: true),
                    lectura_anterior = table.Column<double>(nullable: false),
                    lectura_actual = table.Column<double>(nullable: false),
                    fecha = table.Column<DateTime>(nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Agua_No_Contabilizada", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "Asada_Queja",
                columns: table => new
                {
                    Id = table.Column<int>(nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.SerialColumn),
                    numero_casa = table.Column<string>(nullable: true),
                    descripcion = table.Column<string>(type: "text", nullable: true),
                    fecha = table.Column<DateTime>(nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Asada_Queja", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "Cloracion",
                columns: table => new
                {
                    Id = table.Column<int>(nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.SerialColumn),
                    fecha = table.Column<DateTime>(nullable: false),
                    hora = table.Column<DateTime>(nullable: false),
                    ppm = table.Column<double>(nullable: false),
                    sector = table.Column<char>(nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Cloracion", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "Cuenta_Bancaria",
                columns: table => new
                {
                    Id = table.Column<int>(nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.SerialColumn),
                    numero_cuenta = table.Column<string>(maxLength: 200, nullable: true),
                    tipo_banco = table.Column<string>(maxLength: 3, nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Cuenta_Bancaria", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "Factura",
                columns: table => new
                {
                    Id = table.Column<int>(nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.SerialColumn),
                    numero_factura = table.Column<string>(maxLength: 200, nullable: true),
                    monto = table.Column<double>(nullable: false),
                    destino = table.Column<bool>(nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Factura", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "Inventario_Stock",
                columns: table => new
                {
                    Id = table.Column<int>(nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.SerialColumn),
                    codigo_articulo = table.Column<string>(maxLength: 50, nullable: true),
                    nombre_articulo = table.Column<string>(maxLength: 200, nullable: true),
                    cantidad = table.Column<int>(nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Inventario_Stock", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "Material",
                columns: table => new
                {
                    Id = table.Column<int>(nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.SerialColumn),
                    material = table.Column<string>(maxLength: 50, nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Material", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "Registro_Compra",
                columns: table => new
                {
                    Id = table.Column<int>(nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.SerialColumn),
                    fecha = table.Column<DateTime>(nullable: false),
                    numero_factura = table.Column<string>(maxLength: 50, nullable: true),
                    provedor = table.Column<string>(maxLength: 50, nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Registro_Compra", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "Registro_Salida",
                columns: table => new
                {
                    Id = table.Column<int>(nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.SerialColumn),
                    fecha = table.Column<DateTime>(nullable: false),
                    motivo = table.Column<string>(type: "text", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Registro_Salida", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "users",
                columns: table => new
                {
                    Id = table.Column<int>(nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.SerialColumn),
                    first_name = table.Column<string>(nullable: true),
                    last_name = table.Column<string>(nullable: true),
                    username = table.Column<string>(maxLength: 50, nullable: false),
                    email = table.Column<string>(maxLength: 100, nullable: false),
                    password = table.Column<string>(maxLength: 200, nullable: false),
                    IsAdmin = table.Column<bool>(nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_users", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "Averia",
                columns: table => new
                {
                    Id = table.Column<int>(nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.SerialColumn),
                    Id_queja = table.Column<int>(nullable: false),
                    numero_averia = table.Column<int>(nullable: false),
                    fecha = table.Column<DateTime>(nullable: false),
                    numero_casa = table.Column<string>(maxLength: 10, nullable: true),
                    numero_medidor = table.Column<string>(maxLength: 150, nullable: true),
                    responsable = table.Column<string>(maxLength: 150, nullable: true),
                    observaciones = table.Column<string>(type: "text", nullable: true),
                    fecha_trabajo = table.Column<DateTime>(nullable: false),
                    costo_mano_obra = table.Column<double>(nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Averia", x => x.Id);
                    table.ForeignKey(
                        name: "FK_Averia_Asada_Queja_Id_queja",
                        column: x => x.Id_queja,
                        principalTable: "Asada_Queja",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "Cheque",
                columns: table => new
                {
                    Id = table.Column<int>(nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.SerialColumn),
                    Id_cuenta = table.Column<int>(nullable: false),
                    fecha = table.Column<DateTime>(nullable: false),
                    numero_cheque = table.Column<string>(maxLength: 100, nullable: true),
                    numero_cuenta = table.Column<string>(maxLength: 200, nullable: true),
                    monto = table.Column<double>(nullable: false),
                    portador = table.Column<string>(maxLength: 50, nullable: true),
                    concepto = table.Column<string>(type: "text", nullable: true),
                    tipo = table.Column<bool>(nullable: false),
                    cuenta_contable = table.Column<string>(maxLength: 250, nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Cheque", x => x.Id);
                    table.ForeignKey(
                        name: "FK_Cheque_Cuenta_Bancaria_Id_cuenta",
                        column: x => x.Id_cuenta,
                        principalTable: "Cuenta_Bancaria",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "Reciclaje",
                columns: table => new
                {
                    Id = table.Column<int>(nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.SerialColumn),
                    Fk_material = table.Column<int>(nullable: false),
                    cantidad = table.Column<int>(nullable: false),
                    precio_kilo = table.Column<double>(nullable: false),
                    fecha = table.Column<DateTime>(nullable: false),
                    numero_boleta = table.Column<int>(nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Reciclaje", x => x.Id);
                    table.ForeignKey(
                        name: "FK_Reciclaje_Material_Fk_material",
                        column: x => x.Fk_material,
                        principalTable: "Material",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "Compra_Inventario",
                columns: table => new
                {
                    Id = table.Column<int>(nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.SerialColumn),
                    Id_compra = table.Column<int>(nullable: false),
                    Id_articulo = table.Column<int>(nullable: false),
                    cantidad = table.Column<int>(nullable: false),
                    fecha = table.Column<DateTime>(nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Compra_Inventario", x => x.Id);
                    table.ForeignKey(
                        name: "FK_Compra_Inventario_Inventario_Stock_Id_articulo",
                        column: x => x.Id_articulo,
                        principalTable: "Inventario_Stock",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_Compra_Inventario_Registro_Compra_Id_compra",
                        column: x => x.Id_compra,
                        principalTable: "Registro_Compra",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "Prestamo_Inventario",
                columns: table => new
                {
                    Id = table.Column<int>(nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.SerialColumn),
                    Id_salida = table.Column<int>(nullable: false),
                    Id_articulo = table.Column<int>(nullable: false),
                    fecha_prestamo = table.Column<DateTime>(nullable: false),
                    fecha_devolucion = table.Column<DateTime>(nullable: false),
                    estado = table.Column<bool>(nullable: false),
                    responsable = table.Column<string>(maxLength: 20, nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Prestamo_Inventario", x => x.Id);
                    table.ForeignKey(
                        name: "FK_Prestamo_Inventario_Inventario_Stock_Id_articulo",
                        column: x => x.Id_articulo,
                        principalTable: "Inventario_Stock",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_Prestamo_Inventario_Registro_Salida_Id_salida",
                        column: x => x.Id_salida,
                        principalTable: "Registro_Salida",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "Salida_Inventario",
                columns: table => new
                {
                    Id = table.Column<int>(nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.SerialColumn),
                    Id_articulo = table.Column<int>(nullable: false),
                    Id_salida = table.Column<int>(nullable: false),
                    cantidad = table.Column<int>(nullable: false),
                    tipo = table.Column<char>(nullable: false),
                    fecha = table.Column<DateTime>(nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Salida_Inventario", x => x.Id);
                    table.ForeignKey(
                        name: "FK_Salida_Inventario_Inventario_Stock_Id_articulo",
                        column: x => x.Id_articulo,
                        principalTable: "Inventario_Stock",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_Salida_Inventario_Registro_Salida_Id_salida",
                        column: x => x.Id_salida,
                        principalTable: "Registro_Salida",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "Inventario_Averia",
                columns: table => new
                {
                    Id = table.Column<int>(nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.SerialColumn),
                    Id_articulo = table.Column<int>(nullable: false),
                    Id_averia = table.Column<int>(nullable: false),
                    cantidad = table.Column<int>(nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Inventario_Averia", x => x.Id);
                    table.ForeignKey(
                        name: "FK_Inventario_Averia_Inventario_Stock_Id_articulo",
                        column: x => x.Id_articulo,
                        principalTable: "Inventario_Stock",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_Inventario_Averia_Averia_Id_averia",
                        column: x => x.Id_averia,
                        principalTable: "Averia",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateTable(
                name: "Factura_Cheque",
                columns: table => new
                {
                    Id = table.Column<int>(nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.SerialColumn),
                    Id_cheque = table.Column<int>(nullable: false),
                    Id_factura = table.Column<int>(nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Factura_Cheque", x => x.Id);
                    table.ForeignKey(
                        name: "FK_Factura_Cheque_Cheque_Id_cheque",
                        column: x => x.Id_cheque,
                        principalTable: "Cheque",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_Factura_Cheque_Factura_Id_factura",
                        column: x => x.Id_factura,
                        principalTable: "Factura",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateIndex(
                name: "IX_Averia_Id_queja",
                table: "Averia",
                column: "Id_queja");

            migrationBuilder.CreateIndex(
                name: "IX_Cheque_Id_cuenta",
                table: "Cheque",
                column: "Id_cuenta");

            migrationBuilder.CreateIndex(
                name: "IX_Compra_Inventario_Id_articulo",
                table: "Compra_Inventario",
                column: "Id_articulo");

            migrationBuilder.CreateIndex(
                name: "IX_Compra_Inventario_Id_compra",
                table: "Compra_Inventario",
                column: "Id_compra");

            migrationBuilder.CreateIndex(
                name: "IX_Factura_Cheque_Id_cheque",
                table: "Factura_Cheque",
                column: "Id_cheque");

            migrationBuilder.CreateIndex(
                name: "IX_Factura_Cheque_Id_factura",
                table: "Factura_Cheque",
                column: "Id_factura");

            migrationBuilder.CreateIndex(
                name: "IX_Inventario_Averia_Id_articulo",
                table: "Inventario_Averia",
                column: "Id_articulo");

            migrationBuilder.CreateIndex(
                name: "IX_Inventario_Averia_Id_averia",
                table: "Inventario_Averia",
                column: "Id_averia");

            migrationBuilder.CreateIndex(
                name: "IX_Prestamo_Inventario_Id_articulo",
                table: "Prestamo_Inventario",
                column: "Id_articulo");

            migrationBuilder.CreateIndex(
                name: "IX_Prestamo_Inventario_Id_salida",
                table: "Prestamo_Inventario",
                column: "Id_salida");

            migrationBuilder.CreateIndex(
                name: "IX_Reciclaje_Fk_material",
                table: "Reciclaje",
                column: "Fk_material");

            migrationBuilder.CreateIndex(
                name: "IX_Salida_Inventario_Id_articulo",
                table: "Salida_Inventario",
                column: "Id_articulo");

            migrationBuilder.CreateIndex(
                name: "IX_Salida_Inventario_Id_salida",
                table: "Salida_Inventario",
                column: "Id_salida");
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "Aforo");

            migrationBuilder.DropTable(
                name: "Agua_No_Contabilizada");

            migrationBuilder.DropTable(
                name: "Cloracion");

            migrationBuilder.DropTable(
                name: "Compra_Inventario");

            migrationBuilder.DropTable(
                name: "Factura_Cheque");

            migrationBuilder.DropTable(
                name: "Inventario_Averia");

            migrationBuilder.DropTable(
                name: "Prestamo_Inventario");

            migrationBuilder.DropTable(
                name: "Reciclaje");

            migrationBuilder.DropTable(
                name: "Salida_Inventario");

            migrationBuilder.DropTable(
                name: "users");

            migrationBuilder.DropTable(
                name: "Registro_Compra");

            migrationBuilder.DropTable(
                name: "Cheque");

            migrationBuilder.DropTable(
                name: "Factura");

            migrationBuilder.DropTable(
                name: "Averia");

            migrationBuilder.DropTable(
                name: "Material");

            migrationBuilder.DropTable(
                name: "Inventario_Stock");

            migrationBuilder.DropTable(
                name: "Registro_Salida");

            migrationBuilder.DropTable(
                name: "Cuenta_Bancaria");

            migrationBuilder.DropTable(
                name: "Asada_Queja");
        }
    }
}