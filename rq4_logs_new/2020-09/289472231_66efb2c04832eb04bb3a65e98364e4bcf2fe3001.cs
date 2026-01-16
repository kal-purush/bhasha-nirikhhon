using System;
using rcLogTransfers;
using rcLogUtils;

namespace rcLogBusiness
{
    public class SistemaBusiness
    {
        public SistemaTransfer Validar(SistemaTransfer sistemaTransfer) 
        {
            SistemaTransfer sistemaValidacao;

            try  {
                sistemaValidacao = new SistemaTransfer(sistemaTransfer);

                //-- Descrição de Cor
                if (string.IsNullOrEmpty(sistemaValidacao.Sistema.Descricao)) {
                    sistemaValidacao.IncluirMensagem("Necessário informar a Descrição da Cor");
                } else if ((sistemaValidacao.Sistema.Descricao.Length < 3) || 
                        (sistemaValidacao.Sistema.Descricao.Length > 100)) {
                    sistemaValidacao.IncluirMensagem("Descrição deve ter entre 3 e 100 caracteres");
                } else if (!Validacao.ValidarCharAaBCcNT(sistemaValidacao.Sistema.Descricao)) {
                    sistemaValidacao.IncluirMensagem("Descrição possui caracteres inválidos");
                    sistemaValidacao.IncluirMensagem("Caracteres válidos: letras, acentos, números, traço e espaço em branco");
                } else if (!Validacao.ValidarBrancoIniFim(sistemaValidacao.Sistema.Descricao)) {
                    sistemaValidacao.IncluirMensagem("Descrição não deve começar ou terminar com espaço em branco");
                }

                //-- Código de Cor
                if (!string.IsNullOrEmpty(sistemaValidacao.Sistema.Codigo)) {
                    if ((sistemaValidacao.Sistema.Codigo.Length < 3) || 
                        (sistemaValidacao.Sistema.Codigo.Length > 10)) {
                        sistemaValidacao.IncluirMensagem("Código deve ter entre 3 e 10 caracteres");
                    } else if(!Validacao.ValidarCharAaNT(sistemaValidacao.Sistema.Codigo)) {
                        sistemaValidacao.IncluirMensagem("Código possui caracteres inválidos");
                        sistemaValidacao.IncluirMensagem("Caracteres válidos: letras, números e traço");
                    }
                }

                sistemaValidacao.Validacao = true;

                if (sistemaValidacao.Mensagens != null) {
                    if (sistemaValidacao.Mensagens.Count > 0) {
                        sistemaValidacao.Validacao = false;
                    }
                }

                sistemaValidacao.Erro = false;
            } catch (Exception ex) {
                sistemaValidacao = new SistemaTransfer();

                sistemaValidacao.IncluirMensagem("Erro em SistemaBusiness Validar [" + ex.Message + "]");
                sistemaValidacao.Validacao = false;
                sistemaValidacao.Erro = true;
            }

            return sistemaValidacao;
        }

        public SistemaTransfer ValidarConsulta(SistemaTransfer sistemaTransfer) 
        {
            SistemaTransfer sistemaValidacao;

            try  {
                sistemaValidacao = new SistemaTransfer(sistemaTransfer);

                if (sistemaValidacao != null) {

                    //-- Id
                    if ((sistemaValidacao.Filtro.IdDe <= 0) && (sistemaValidacao.Filtro.IdAte > 0)) {
                        sistemaValidacao.IncluirMensagem("Informe apenas o Id (De) para consultar um Id específico, ou os valores De e Até para consultar uma faixa de Id");
                    } else if ((sistemaValidacao.Filtro.IdDe > 0) && (sistemaValidacao.Filtro.IdAte > 0)) {
                        if (sistemaValidacao.Filtro.IdDe >= sistemaValidacao.Filtro.IdAte) {
                            sistemaValidacao.IncluirMensagem("O valor mínimo (De) do Id deve ser menor que o valor máximo (Até)");
                        }
                    }

                    //-- Descrição de Cor
                    if (!string.IsNullOrEmpty(sistemaValidacao.Filtro.Descricao)) {
                        if (sistemaValidacao.Filtro.Descricao.Length > 100) {
                            sistemaValidacao.IncluirMensagem("Descrição deve ter no máximo 100 caracteres");
                        } else if (!Validacao.ValidarCharAaBCcNT(sistemaValidacao.Filtro.Descricao)) {
                            sistemaValidacao.IncluirMensagem("Descrição possui caracteres inválidos");
                            sistemaValidacao.IncluirMensagem("Caracteres válidos: letras, acentos, números, traço e espaço em branco");
                        }
                    }

                    //-- Código de Cor
                    if (!string.IsNullOrEmpty(sistemaValidacao.Filtro.Codigo)) {
                        if (sistemaValidacao.Filtro.Codigo.Length > 10) {
                            sistemaValidacao.IncluirMensagem("Código deve ter no máximo 10 caracteres");
                        } else if(!Validacao.ValidarCharAaNT(sistemaValidacao.Filtro.Codigo)) {
                            sistemaValidacao.IncluirMensagem("Código possui caracteres inválidos");
                            sistemaValidacao.IncluirMensagem("Caracteres válidos: letras, números e traço");
                        }
                    }

                    //-- Data de Criação
                    if ((sistemaValidacao.Filtro.CriacaoDe == DateTime.MinValue) && (sistemaValidacao.Filtro.CriacaoAte != DateTime.MinValue)) {
                        sistemaValidacao.IncluirMensagem("Informe apenas a Data de Criação (De) para consultar uma data específica, ou os valores De e Até para consultar uma faixa de datas");
                    } else if ((sistemaValidacao.Filtro.CriacaoDe > DateTime.MinValue) && (sistemaValidacao.Filtro.CriacaoAte > DateTime.MinValue)) {
                        if (sistemaValidacao.Filtro.CriacaoDe >= sistemaValidacao.Filtro.CriacaoAte) {
                            sistemaValidacao.IncluirMensagem("O valor mínimo (De) da Data de Criação deve ser menor que o valor máximo (Até)");
                        }
                    }

                    //-- Data de Alteração
                    if ((sistemaValidacao.Filtro.AlteracaoDe == DateTime.MinValue) && (sistemaValidacao.Filtro.AlteracaoAte != DateTime.MinValue)) {
                        sistemaValidacao.IncluirMensagem("Informe apenas a Data de Alteração (De) para consultar uma data específica, ou os valores De e Até para consultar uma faixa de datas");
                    } else if ((sistemaValidacao.Filtro.AlteracaoDe > DateTime.MinValue) && (sistemaValidacao.Filtro.AlteracaoAte > DateTime.MinValue)) {
                        if (sistemaValidacao.Filtro.AlteracaoDe >= sistemaValidacao.Filtro.AlteracaoAte) {
                            sistemaValidacao.IncluirMensagem("O valor mínimo (De) da Data de Alteração deve ser menor que o valor máximo (Até)");
                        }
                    }
                } else {
                    sistemaValidacao = new SistemaTransfer();
                    sistemaValidacao.IncluirMensagem("É necessário informar os dados da Cor");
                }

                sistemaValidacao.Validacao = true;

                if (sistemaValidacao.Mensagens != null) {
                    if (sistemaValidacao.Mensagens.Count > 0) {
                        sistemaValidacao.Validacao = false;
                    }
                }

                sistemaValidacao.Erro = false;
            } catch (Exception ex) {
                sistemaValidacao = new SistemaTransfer();

                sistemaValidacao.IncluirMensagem("Erro em SistemaBusiness Validar [" + ex.Message + "]");
                sistemaValidacao.Validacao = false;
                sistemaValidacao.Erro = true;
            }

            return sistemaValidacao;
        }
    }
}