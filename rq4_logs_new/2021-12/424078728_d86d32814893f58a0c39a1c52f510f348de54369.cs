using System;
using Microsoft.EntityFrameworkCore.Migrations;

namespace Gestao_de_Projetos.Migrations
{
    public partial class projetos : Migration
    {
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "Clientes",
                columns: table => new
                {
                    ClientesId = table.Column<int>(nullable: false)
                        .Annotation("SqlServer:Identity", "1, 1"),
                    Nome = table.Column<string>(maxLength: 20, nullable: false),
                    Apelido = table.Column<string>(maxLength: 20, nullable: false),
                    Contacto = table.Column<string>(maxLength: 20, nullable: true),
                    NIF = table.Column<string>(maxLength: 10, nullable: true),
                    Email = table.Column<string>(maxLength: 50, nullable: false),
                    Password = table.Column<string>(maxLength: 20, nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Clientes", x => x.ClientesId);
                });

            migrationBuilder.CreateTable(
                name: "Estado_Projeto",
                columns: table => new
                {
                    ID_Estado = table.Column<int>(nullable: false)
                        .Annotation("SqlServer:Identity", "1, 1"),
                    Nome = table.Column<string>(nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Estado_Projeto", x => x.ID_Estado);
                });

            migrationBuilder.CreateTable(
                name: "Funcao",
                columns: table => new
                {
                    ID_funcao = table.Column<int>(nullable: false)
                        .Annotation("SqlServer:Identity", "1, 1"),
                    Nome_Funcao = table.Column<string>(nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Funcao", x => x.ID_funcao);
                });

            migrationBuilder.CreateTable(
                name: "Projetos",
                columns: table => new
                {
                    ID_projeto = table.Column<int>(nullable: false)
                        .Annotation("SqlServer:Identity", "1, 1"),
                    ClientesId = table.Column<int>(nullable: false),
                    Nome_projeto = table.Column<string>(nullable: false),
                    Estado_ProjetoID_Estado = table.Column<int>(nullable: true),
                    ID_Estado = table.Column<int>(nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Projetos", x => x.ID_projeto);
                    table.ForeignKey(
                        name: "FK_Projetos_Clientes_ClientesId",
                        column: x => x.ClientesId,
                        principalTable: "Clientes",
                        principalColumn: "ClientesId",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_Projetos_Estado_Projeto_Estado_ProjetoID_Estado",
                        column: x => x.Estado_ProjetoID_Estado,
                        principalTable: "Estado_Projeto",
                        principalColumn: "ID_Estado",
                        onDelete: ReferentialAction.Restrict);
                });

            migrationBuilder.CreateTable(
                name: "Membros",
                columns: table => new
                {
                    ID_membro = table.Column<int>(nullable: false)
                        .Annotation("SqlServer:Identity", "1, 1"),
                    ID_funcao = table.Column<int>(nullable: false),
                    FuncaoID_funcao = table.Column<int>(nullable: true),
                    Nome_membro = table.Column<string>(nullable: false),
                    Sobrenome = table.Column<string>(nullable: false),
                    Telefone = table.Column<int>(nullable: false),
                    NIF = table.Column<string>(maxLength: 10, nullable: true),
                    Email = table.Column<string>(maxLength: 50, nullable: false),
                    Password = table.Column<string>(maxLength: 20, nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Membros", x => x.ID_membro);
                    table.ForeignKey(
                        name: "FK_Membros_Funcao_FuncaoID_funcao",
                        column: x => x.FuncaoID_funcao,
                        principalTable: "Funcao",
                        principalColumn: "ID_funcao",
                        onDelete: ReferentialAction.Restrict);
                });

            migrationBuilder.CreateTable(
                name: "MembroProjeto",
                columns: table => new
                {
                    ID_MembroProjeto = table.Column<int>(nullable: false)
                        .Annotation("SqlServer:Identity", "1, 1"),
                    ID_projeto = table.Column<int>(nullable: false),
                    ProjetoID_projeto = table.Column<int>(nullable: true),
                    ID_membro = table.Column<int>(nullable: false),
                    MembrosID_membro = table.Column<int>(nullable: true),
                    DataInicio = table.Column<DateTime>(nullable: false),
                    DataPrevistaFim = table.Column<DateTime>(nullable: false),
                    DataFim = table.Column<DateTime>(nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_MembroProjeto", x => x.ID_MembroProjeto);
                    table.ForeignKey(
                        name: "FK_MembroProjeto_Membros_MembrosID_membro",
                        column: x => x.MembrosID_membro,
                        principalTable: "Membros",
                        principalColumn: "ID_membro",
                        onDelete: ReferentialAction.Restrict);
                    table.ForeignKey(
                        name: "FK_MembroProjeto_Projetos_ProjetoID_projeto",
                        column: x => x.ProjetoID_projeto,
                        principalTable: "Projetos",
                        principalColumn: "ID_projeto",
                        onDelete: ReferentialAction.Restrict);
                });

            migrationBuilder.CreateTable(
                name: "Tarefas",
                columns: table => new
                {
                    IdTarefas = table.Column<int>(nullable: false)
                        .Annotation("SqlServer:Identity", "1, 1"),
                    ID_projeto = table.Column<int>(nullable: false),
                    ProjetosID_projeto = table.Column<int>(nullable: true),
                    Nome_Tarefa = table.Column<string>(nullable: false),
                    Descricao = table.Column<string>(nullable: false),
                    DataPrevistaInicio = table.Column<DateTime>(nullable: false),
                    DataPrevistaFim = table.Column<DateTime>(nullable: false),
                    ID_membro = table.Column<int>(nullable: false),
                    MembrosID_membro = table.Column<int>(nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Tarefas", x => x.IdTarefas);
                    table.ForeignKey(
                        name: "FK_Tarefas_Membros_MembrosID_membro",
                        column: x => x.MembrosID_membro,
                        principalTable: "Membros",
                        principalColumn: "ID_membro",
                        onDelete: ReferentialAction.Restrict);
                    table.ForeignKey(
                        name: "FK_Tarefas_Projetos_ProjetosID_projeto",
                        column: x => x.ProjetosID_projeto,
                        principalTable: "Projetos",
                        principalColumn: "ID_projeto",
                        onDelete: ReferentialAction.Restrict);
                });

            migrationBuilder.CreateTable(
                name: "MembroTarefa",
                columns: table => new
                {
                    ID_MembroTarefa = table.Column<int>(nullable: false)
                        .Annotation("SqlServer:Identity", "1, 1"),
                    IdTarefas = table.Column<int>(nullable: false),
                    TarefasIdTarefas = table.Column<int>(nullable: true),
                    ID_membro = table.Column<int>(nullable: false),
                    MembrosID_membro = table.Column<int>(nullable: true),
                    DataPrevistaInicio = table.Column<DateTime>(nullable: false),
                    DataEfetivaInicio = table.Column<DateTime>(nullable: false),
                    DataPrevistaFim = table.Column<DateTime>(nullable: false),
                    DataEfetivaFim = table.Column<DateTime>(nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_MembroTarefa", x => x.ID_MembroTarefa);
                    table.ForeignKey(
                        name: "FK_MembroTarefa_Membros_MembrosID_membro",
                        column: x => x.MembrosID_membro,
                        principalTable: "Membros",
                        principalColumn: "ID_membro",
                        onDelete: ReferentialAction.Restrict);
                    table.ForeignKey(
                        name: "FK_MembroTarefa_Tarefas_TarefasIdTarefas",
                        column: x => x.TarefasIdTarefas,
                        principalTable: "Tarefas",
                        principalColumn: "IdTarefas",
                        onDelete: ReferentialAction.Restrict);
                });

            migrationBuilder.CreateIndex(
                name: "IX_MembroProjeto_MembrosID_membro",
                table: "MembroProjeto",
                column: "MembrosID_membro");

            migrationBuilder.CreateIndex(
                name: "IX_MembroProjeto_ProjetoID_projeto",
                table: "MembroProjeto",
                column: "ProjetoID_projeto");

            migrationBuilder.CreateIndex(
                name: "IX_Membros_FuncaoID_funcao",
                table: "Membros",
                column: "FuncaoID_funcao");

            migrationBuilder.CreateIndex(
                name: "IX_MembroTarefa_MembrosID_membro",
                table: "MembroTarefa",
                column: "MembrosID_membro");

            migrationBuilder.CreateIndex(
                name: "IX_MembroTarefa_TarefasIdTarefas",
                table: "MembroTarefa",
                column: "TarefasIdTarefas");

            migrationBuilder.CreateIndex(
                name: "IX_Projetos_ClientesId",
                table: "Projetos",
                column: "ClientesId");

            migrationBuilder.CreateIndex(
                name: "IX_Projetos_Estado_ProjetoID_Estado",
                table: "Projetos",
                column: "Estado_ProjetoID_Estado");

            migrationBuilder.CreateIndex(
                name: "IX_Tarefas_MembrosID_membro",
                table: "Tarefas",
                column: "MembrosID_membro");

            migrationBuilder.CreateIndex(
                name: "IX_Tarefas_ProjetosID_projeto",
                table: "Tarefas",
                column: "ProjetosID_projeto");
        }

        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "MembroProjeto");

            migrationBuilder.DropTable(
                name: "MembroTarefa");

            migrationBuilder.DropTable(
                name: "Tarefas");

            migrationBuilder.DropTable(
                name: "Membros");

            migrationBuilder.DropTable(
                name: "Projetos");

            migrationBuilder.DropTable(
                name: "Funcao");

            migrationBuilder.DropTable(
                name: "Clientes");

            migrationBuilder.DropTable(
                name: "Estado_Projeto");
        }
    }
}