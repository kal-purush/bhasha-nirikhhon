/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package combate;

/**
 *
 * @author Rosita
 */
/*
*  Para que se pueda producir un asalto tengo que tener el portón abierto,
*  llevar torres de asalto, escalas o la muralla deteriorada.
*/
public class Asalto 
{
    private int tropasenTorre=200;
    private int plomosporTorre=5;
    private int [] iterador={5,3,0,4};
    
    
    
    
    // La variable estado es del estado de conservación de la muralla, pues se supone
    // que si hay asalto es porque el portón está roto o la muralla deteriorada.
    public void asaltar(ReporteAsalto reporte, GrupoTropas ataca, GrupoTropas guarnecido, Edificio edificio, pCombate p)
    {
        double estado=edificio.getEstado()/100.0;
        int ntropas=reporte.getnTropas();
        GrupoTropas torres=new GrupoTropas();
        
        //Para guardar el poder de las tropas asaltantes.
        double [] poderA=new double[ntropas+1];
        double [] poderTorres=new double[ntropas+1];
        //Para guardar la defensa de las tropas asaltantes
        double [] defensaA=new double[ntropas];
        double [] defensaTorres=new double[ntropas+1];
        
        //Obtenemos el poder y defensa de las unidades guarnecidas
        double [] poderG=guarnecido.getPoderGuarnicion(estado,p);
        double [] defensaG=guarnecido.getDefensaGuarnicion(estado,p);       
        
/************************* Combate con las torres ***************************************************/
        //Comprobamos si el atacante posee torres de asedio 
        if(ataca.isTorres())
        {
            //Obtenemos la composición de las unidades que suben por las torres
            formacionTorres(reporte,ataca,torres,edificio.getNplomos());
            //Obtenemos los poderes de ataque y defensa con las bonificaciones de las murallas
            poderTorres=torres.getPoderGuarnicion(estado,p);
            defensaTorres=torres.getDefensaGuarnicion(estado,p);
        }
/**************************** Combate en muralla deteriorada y portón***********************************************/
        //Comprobamos si entran también tropas por la puerta o la muralla y calculamos sus poderes
        if((edificio.getPortonAbierto())||(edificio.getAsaltable()))
        {
            //Obtenemos el poder y defensa de las tropas que entran por el portón o la murralla
            poderA=ataca.getPoderAsalto(p);
            defensaA=ataca.getDefensaAsalto(p);
        }
            
/*********************** Continuamos con el desarrollo del combate ******************/        
        //Juntamos los poderes de las tropas que van por las torres con las demás
        poderA[ntropas]=poderA[ntropas]+poderTorres[ntropas];
        //Obtenemos la condición de victoria
        double auxA=poderA[ntropas]/(poderA[ntropas]+poderG[ntropas]);
        double auxD=poderG[ntropas]/(poderA[ntropas]+poderG[ntropas]);
        
        
        //Metemos las condiciones de victoria en el reporte
        //Enfrentamos el poder de ataque de los atacantes con el poder defensivo de las unidades guarnecidas.
        if(poderA[ntropas]>defensaG[ntropas])
        {
            //Gana el atacante
            reporte.setvictoriaatacante(true);
            
            //Como las tropas guarnecidas no pueden huir subimos la cota de aniquilación
            if(auxD<=0.1)
            {
                //Aniquilación de D
                reporte.setdefensoraniquilado(true);
                //Calculamos las bajas
                for(int i=0;i<ntropas;i++)
                {
                    if(poderA[i]>0)
                    {
                        reporte.setbajasatacante(redondea(0.2*auxD*poderA[i]/defensaA[i]),i);
                    }
                    if(poderTorres[i]>0)
                    {
                        reporte.setbajasatacante(redondea(0.2*auxD*poderTorres[i]/defensaTorres[i]),i);
                    }        
                    reporte.setbajasdefensor(guarnecido.get(i).getcantidad(),i);                 
                } 
                return;
            }           
        }
        else
        {
            //Gana el grupo que defiende, en caso de empoate le damos la ventaja al defensor
            reporte.setvictoriadefensor(true);
            if(auxA<=0.02)
            {
                //Aniquilación de A
                reporte.setatacanteaniquilado(true);
                //Calculamos las bajas
                for(int i=0;i<ntropas;i++)
                {
                    if(poderA[i]>0)
                    {
                        reporte.setbajasatacante(ataca.get(i).getcantidad(),i);
                    }
                    if(poderTorres[i]>0)
                    {
                        reporte.setbajasatacante(ataca.get(i).getcantidad(),i);
                    }        
                    if(poderG[i]>0)
                    {
                        reporte.setbajasdefensor(redondea(0.15*auxA*poderG[i]/defensaG[i]),i);
                    }                 
                } 
                return;
            }
            else
            {
                if(auxA<=0.2)
                {
                    //Retirada al principal o al personaje
                    reporte.sethuyeatacante(true);
                }
                else
                {
                    if(auxA<=0.35)
                    {
                        //Se mueve a posiciones mas seguras.
                        reporte.setmoveratacante(true);
                    }
                }
            }
        }
        
        //Obtenemos la bonificaciones por victoria
        double rba=0.0;//Reducción de bajas del atacante.
        double rbd=0.0;//Reducción de bajas del defensor.
        
        //En caso de empate técnico repartimos las bajas
        if((poderA[ntropas]>=0.47)&&(poderA[ntropas]<=0.53))
        {
            rba=0.5;
            rbd=0.5;
        }
        else
        {      
            if(reporte.isVictoriaatacante())
            {
                rba=0.3;
                rbd=0.7;
            }
            else
            {
                rba=0.7;
                rbd=0.3;
            }
        }

        //Calculamos las bajas
        for(int i=0;i<ntropas;i++)
        {
            if(poderA[i]>0)
            {
                reporte.setbajasatacante(redondea(0.5*auxD*rba*poderA[i]/defensaA[i]),i);
            }
            if(poderTorres[i]>0)
            {
                reporte.setbajasatacante(redondea(0.5*auxD*rba*poderTorres[i]/defensaTorres[i]),i);
            }
            if(poderG[i]>0)
            {
                reporte.setbajasdefensor(redondea(0.5*auxA*rbd*poderG[i]/defensaG[i]),i);
            }
        } 
    }
    
    //Función que dada un GrupoTropas y los plomos del enemigo devuelve el GrupoTropas con 
    //las unidades que suben por las torres en el asedio, descontando las bajas que producen los plomos
    
    public void formacionTorres(ReporteAsalto reporte, GrupoTropas ataca, GrupoTropas torres, int nplomos)
    {
        double pbajas=0.0;

        //En primer lugar obtenemos los parámetros del plomo fundido
        if(nplomos>0)
        {
             //Obtenemos el número máximo de plomos que podemos usar.
             int nmax=Math.min(nplomos,plomosporTorre*ataca.getNtorres());
               
             //Obtenemos el porcentaje de daño causado a las torres.
             reporte.setdanoTorres(redondea(Math.random()*20*ataca.getNtorres()+Math.random()*10*nmax));
                
             //Obtenemos el porcentaje de daño causado a las tropas.
             pbajas=(0.05+Math.random()*0.1)*nmax/ataca.getNtorres();
             //Reportamos los plomos gastados
             reporte.setplomosGastados(nmax);
         }
               
         //Obtenemos la composición y cantidad de las unidades que van en las torres.
         int nmax=tropasenTorre*ataca.getNtorres();//Número máximo de unidades que suben por las torres.
            
         for(int i=0;i<iterador.length;i++)
         {
             if(nmax>0)
             {
                    //Obtenemos el número de unidades que suben a la torre.
                    int aux=Math.min(nmax, ataca.get(iterador[i]).getcantidad());
                    //Quitamos del GrupoTropas las unidades que suben a la torre.
                    ataca.get(iterador[i]).setbajas(aux);
                    //Calculamos el número de bajas que se producen por los plomos
                    int bajas=redondea(aux*pbajas);
                    //Actualizamos las bajas en el reporte
                    reporte.setbajasatacante(bajas,iterador[i]);
                    //Actualizamos el contador
                    nmax=nmax-aux;
                    //Obtenemos las unidades que quedan
                    aux=aux-bajas;
                    //Metemos en el GrupoTropas las unidades que entran definitivamente
                    torres.set(iterador[i],aux,ataca.get(iterador[i]).getmoral(),ataca.get(iterador[i]).getpericia());
             }
         }
    }
    
    //Dado un double lo redondea.   
    public int redondea(double num)
    {
        int sol=(int)(num);
        if(num-sol<0.5)
        {
              return sol;
        }
        return sol+1;
    }
   //Función que nos da el resultado de lanzar los arietes contra un edificio
    
    //Función que nos da el resultado de disparar las catapultas contra un edificio 
    /* En esta función falta:
     * Actualizar las bajas en las catapultas.
     * Aumentar la pericia de las catapultas tras los disparos.
     * Actualizar el estado del edificio.
     * Hacerlo asaltable en su caso.
     * Actualizar las bajas en las catapultas defensivas.
     * Enviar respectivos mensajes de lo ocurrido.
     * La función se podría modificar para que devolviese el entero del deterioro del edificio
     * Y luego actualizar los demás parámetros de la clase tropas y edificio para actualizar lo demás.
     * También se podría hacer una clase reporte si te es más cómodo, como tú veas.
     */
    void disparaCatapultas(TropasK catapultas, Edificio edificio)
    {
        //Primero actúan las catapultas defensivas
        if(edificio.hayCatapultas())
        {
            int bajas=redondea((0.1+Math.random()*0.15)*edificio.getNcatapultas());
            catapultas.setbajas(bajas);
        }
        //Obtenemos la cantidad de impactos sobre el edificio
        double impacto=2*catapultas.getPoderUnidades()*edificio.resistencia();
        
        //Actualizamos el estado del edificio.
        if(impacto>20)
        {
            edificio.rompeEdificio(20);
            //Actualizamos las bajas en las catapultas defensivas.
            if(edificio.hayCatapultas())
            {
                edificio.setBajasCatapultas(20);
            }
        }
        else
        {
            int aux=redondea(impacto);
            edificio.rompeEdificio(aux);
            //Actualizamos las bajas en las catapultas defensivas.
            if(edificio.hayCatapultas())
            {
                edificio.setBajasCatapultas(aux);
            }
        }
        //Comprobamos si el edificio es ahora asaltable.
        if(edificio.getEstado()<50)
        {
            edificio.setAsaltable(true);
        }  
    }
    //Eta función desarrolla el lanzamiento de los arietes a un edificio.
    //El objeto TropasK se puede sustituir por un entero con el número de arietes.
    public void lanzarArietes(TropasK arietes, Edificio edificio)
    {
        if(edificio.getTipoEdificio()==0)
        {
            edificio.setPortonAbierto(true);
            //Mandamos una baja a los arietes
            arietes.setbajas(1);
            return;
        }
        //Obtenemos el número de arietes que se lanzan
        int narietes=Math.min(edificio.nmaxArietes(),arietes.getcantidad());
        int piedras=edificio.getNpiedras();
        
        if(piedras>3*narietes)
        {
            //Son repelidos todos los arietes.
            arietes.setbajas(narietes);
            edificio.setBajasPiedras(narietes);
            return;
        }
        
        //Liquidamos las piedras, pues se usan todas.
        edificio.setBajasPiedras(narietes);
        //Obtenemos los arietes destruidos por bloques de tres piedras.
        narietes=narietes-(int)(piedras/3.0);
        //Obtenemos las piedras que quedan sueltas.
        piedras=piedras%3;
        //comprobamos si las piedras restantes son capaces de romper otro ariete.
        if(piedras*Math.random()>Math.random())
        {
            narietes--;
        }
        //Comprobamos si se habre el portón
        if(narietes>=edificio.arietesNecesarios())
        {
            edificio.setPortonAbierto(true);
            arietes.setbajas(edificio.arietesNecesarios());
        }   
    }
}