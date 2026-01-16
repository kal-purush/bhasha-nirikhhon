using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Web;
using Servidor_WebService;

/**
 * Função responsável pelo objeto cliente.
 * Nesta classe encontra-se o id do cliente e os vetores responsáveis por armazenar as operações que o usuário tem interesse 
 * em relizar e as operações já realizadas.
 * Além disso essa função inicializa uma thread que realiza a troca de operações entre os vetores de operações e operações realizadas.
 * 
 **/


public class Cliente
{
    int id_cliente;
    List<Operacao> operacoes;
    List<Operacao> operacoes_completadas;

    Servidor_WebService.Servidor_WebService proxy;


    private ThreadStart threadOperacao_ref;
    private Thread threadOperacao;




    public Cliente(int id)
    {
        id_cliente = id;
        operacoes = new List<Operacao>(10);
        operacoes_completadas = new List<Operacao>(10);
        //proxy = proxy_web;
        proxy = new Servidor_WebService.Servidor_WebService();



    }

    /*******************************************************************************************/
    /* GETTERS e SETTERS                                                                       */
    /*******************************************************************************************/

    public int Get_id_usuario()
    {
        return id_cliente;
    }

    public void Set_id_usuario(int id)
    {
        id_cliente = id;
    }

    public Operacao[] GetOperacoesCompletadas()
    {
        return operacoes_completadas.ToArray();
    }
    

    /*******************************************************************************************/
    /* THREAD                                                                                  */
    /*******************************************************************************************/

    /**
     * Função para adicionar uma compra ou venda no vetor de operações
     **/

    public void adiciona_operacao(String id_operacao, string nomeEmpresa, int id_usuario)
    {
        Operacao o = new Operacao(id_operacao, nomeEmpresa, id_usuario);
        operacoes.Add(o);
    }
    
    /**
     *Função responsável por inicializar as threads.
     * Verifica se a thread já existe e se parou de executar para, então, inciliza-las.
     **/
    public void inicializa_threads_operacoes()
    {
        if (threadOperacao == null) { 

            threadOperacao_ref = new ThreadStart(Thread_consulta_operacao);
            threadOperacao = new Thread(threadOperacao_ref);
            threadOperacao.Start();
        }
        if(threadOperacao.ThreadState == ThreadState.Unstarted)
        {
            
        }
    }

    /**
     *Função responsável por verificar se as operações, que o cliente cadastrou interesse, já foram realizadas.
     * Se já foram realizadas transfere do vetor de operações para o de operaçoes realizadas.
     */

    public void Thread_consulta_operacao()
    {
        int nOper = operacoes.Count;
        Operacao[] Op = operacoes.ToArray();


        try
        {
            //while (nOper > 0)
            while(true)
            {
                Op = operacoes.ToArray();

                for(int i = 0; i < Op.Count(); i++)
                {
                    if (proxy.verificar_situacao_CompraVenda(Op[i].Id_operacao))
                    {
                        operacoes_completadas.Add(Op[i]);
                        operacoes.Remove(Op[i]);
                    }
                }
                nOper = operacoes.Count();
                Thread.Sleep(5000);
            }
            //threadOperacao.Suspend();
        }
        catch (ThreadAbortException e)
        {
            Console.WriteLine("Thread Abort Exception");
        }
    }


}