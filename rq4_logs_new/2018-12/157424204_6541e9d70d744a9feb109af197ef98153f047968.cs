using System;
using System.Collections.Generic;
using TaCertoForms.Models;
namespace TaCertoForms.Models{
    public class DesafioDeFaseFactory{
        public List<IDesafioDeFase> GetDesafiosByFaseId(int idFase){
            IDesafioDeFase des1 = null, des2 = null, des3 = null, des4 = null, des5 = null;
            List<IDesafioDeFase> listaDeDesafios = new List<IDesafioDeFase>();

            if(idFase == 1){
                des1 = new DesafioDeFaseNormal();
                des1.Id = 100;
                des1.FaseId = idFase;
                des1.Palavra = "Cambito";
                des1.eCorreto = true;
                des1.Significado = "é uma canela";
                des1.Dica = "antes de p e b se usa m";
                des2 = new DesafioDeFaseNormal();
                des3 = new DesafioDeFaseNormal();
                des4 = new DesafioDeFaseNormal();
                des5 = new DesafioDeFaseNormal();
            }
            else if(idFase == 2){
                des1 = new DesafioDeFaseLacuna();
                des2 = new DesafioDeFaseLacuna();
                des3 = new DesafioDeFaseLacuna();
                des4 = new DesafioDeFaseLacuna();
                des5 = new DesafioDeFaseLacuna();
            }
            else if(idFase == 3){
                des1 = new DesafioDeFaseAurelio();
                des2 = new DesafioDeFaseAurelio();
                des3 = new DesafioDeFaseAurelio();
                des4 = new DesafioDeFaseAurelio();
                des5 = new DesafioDeFaseAurelio();
            }
            if(idFase >= 1 && idFase <= 3){
                listaDeDesafios.Add(des1);
                listaDeDesafios.Add(des2);
                listaDeDesafios.Add(des3);
                listaDeDesafios.Add(des4);
                listaDeDesafios.Add(des5);
            }
            return listaDeDesafios;
        }

    }
}