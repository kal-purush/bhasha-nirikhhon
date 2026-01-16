using System;
using rcLogDatas;
using rcLogDatabase;
using rcLogTransfers;

namespace rcLogDataModels
{
    public class SistemaDataModel : DataModel
    {
        public SistemaTransfer Incluir(SistemaTransfer sistemaTransfer)
        {
            SistemaData sistemaData;
            SistemaTransfer sistema;

            try {
                sistemaData = new SistemaData(db);
                sistema = new SistemaTransfer(sistemaTransfer);

                sistemaData.Incluir(sistema);

                db.ConfirmarTransacao();
            } catch (Exception ex) {
                db.CancelarTransacao();
                
                sistema = new SistemaTransfer();

                sistema.Erro = true;
                sistema.IncluirMensagem("Erro em SistemaDataModel Incluir [" + ex.Message + "]");
            } finally {
                sistemaData = null;
            }

            return sistema;
        }

        // public SistemaTransfer Alterar(SistemaTransfer sistemaTransfer)
        // {
        //     SistemaData sistemaData;
        //     SistemaTransfer sistema;

        //     try {
        //         sistemaData = new SistemaData(_contexto);
        //         sistema = new SistemaTransfer();

        //         sistemaData.Alterar(sistemaTransfer.Cor);

        //         _contexto.SaveChanges();

        //         sistema.Cor = new CorEntity(sistemaTransfer.Cor);
        //         sistema.Validacao = true;
        //         sistema.Erro = false;
        //     } catch (Exception ex) {
        //         sistema = new SistemaTransfer();

        //         sistema.Validacao = false;
        //         sistema.Erro = true;
        //         sistema.IncluirMensagem("Erro em SistemaDataModel Alterar [" + ex.Message + "]");
        //     } finally {
        //         sistemaData = null;
        //     }

        //     return sistema;
        // }

        // public SistemaTransfer Excluir(int id)
        // {
        //     SistemaData sistemaData;
        //     SistemaTransfer sistema;

        //     try {
        //         sistemaData = new SistemaData(_contexto);
        //         sistema = new SistemaTransfer();

        //         sistema.Cor = sistemaData.ConsultarPorId(id);
        //         sistemaData.Excluir(sistema.Cor);

        //         _contexto.SaveChanges();

        //         sistema.Validacao = true;
        //         sistema.Erro = false;
        //     } catch (Exception ex) {
        //         sistema = new SistemaTransfer();

        //         sistema.Validacao = false;
        //         sistema.Erro = true;
        //         sistema.IncluirMensagem("Erro em SistemaDataModel Excluir [" + ex.Message + "]");
        //     } finally {
        //         sistemaData = null;
        //     }

        //     return sistema;
        // }

        // public SistemaTransfer ConsultarPorId(int id)
        // {
        //     SistemaData sistemaData;
        //     SistemaTransfer sistema;

        //     try {
        //         sistemaData = new SistemaData(_contexto);
        //         sistema = new SistemaTransfer();

        //         sistema.Cor = sistemaData.ConsultarPorId(id);
        //         sistema.Validacao = true;
        //         sistema.Erro = false;
        //     } catch (Exception ex) {
        //         sistema = new SistemaTransfer();

        //         sistema.Validacao = false;
        //         sistema.Erro = true;
        //         sistema.IncluirMensagem("Erro em SistemaDataModel ConsultarPorId [" + ex.Message + "]");
        //     } finally {
        //         sistemaData = null;
        //     }

        //     return sistema;
        // }

        // public SistemaTransfer Consultar(SistemaTransfer sistemaTransfer)
        // {
        //     SistemaData sistemaData;
        //     SistemaTransfer corLista;

        //     try {
        //         sistemaData = new SistemaData(_contexto);

        //         corLista = sistemaData.Consultar(sistemaTransfer);
        //         corLista.Validacao = true;
        //         corLista.Erro = false;
        //     } catch (Exception ex) {
        //         corLista = new SistemaTransfer();

        //         corLista.Validacao = false;
        //         corLista.Erro = true;
        //         corLista.IncluirMensagem("Erro em SistemaDataModel Consultar [" + ex.Message + "]");
        //     } finally {
        //         sistemaData = null;
        //     }

        //     return corLista;
        // }
    }
}