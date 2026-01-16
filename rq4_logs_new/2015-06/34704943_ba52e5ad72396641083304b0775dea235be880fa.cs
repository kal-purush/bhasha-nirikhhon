using Microsoft.Xna.Framework;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;

namespace Game.Agentes
{
    public abstract class AgenteAprende : Agent
    {
        private Vector2 posPlayer;
        private Vector2 posAgent;
        private Bloque area;
        private List<Vector2> camino;

        public AgenteAprende(Microsoft.Xna.Framework.Game game, Vector2 tamano, Vector2 posicion, String nombreImagen)
            : base(game, tamano, posicion, nombreImagen)
        {
            NombreImagen = nombreImagen;
            ColorImagen = Color.White;
            posPlayer = new Vector2(-1, -1);
            posAgent = new Vector2(-1, -1);
            area = new Bloque();
            LoadContent();
        }

        private void CalcularCaminoThread()
        {
            foreach (var item in area.sector)
            {
                //Debug.Write(item.name+":"+item.value+" ");
                if (item.name.Equals("player"))
                {
                    acciones action = new acciones();
                    action = Camino(area);
                    Comportamiento(action);
                }
            }
            //Debug.WriteLine("");
            

        }
        // --- funcion sensor 
        public override void Sensor(Bloque area)
        {
            this.area = area;
            // crea un hilo para calcular el camino optimo
            //Thread cal = new Thread(new ThreadStart(CalcularCaminoThread));
            //Thread cal = new Thread(new ThreadStart(SensorAgente));
            //cal.Start();
            SensorAgente();
        }
        // --- funcion SensorAgente que aprende
        private void SensorAgente()
        {
            foreach (var item in area.sector)
            {
                //Debug.Write(item.name+":"+item.value+" ");
                if (item.name.Equals("player"))
                {
                    String accion = CaminoActualizado(area);
                    Bloque estado = disminuirArea();
                    float recompensa = evaluarRecompensa(accion);
                    accion = funcionQ(accion, estado, recompensa); // algoritmo que aprende
                    Debug.WriteLine(accion);
                    Comportamiento(accion);
                }
            }
            //Debug.WriteLine("");
        }
        // --- funcion evaluarRecompensa
        private float evaluarRecompensa(String accion)
        {
            float Out;
            // recompensa para avanzarDer, avanzarIzq, saltar es de -0.03
            if ((accion.Equals("avanzarDer")) || (accion.Equals("avanzarIzq")))
            {
                Out = -0.3f;
            }
            // recompensa para saltarIzq, saltarDer es de -0.02
            else if ((accion.Equals("saltarDer")) || (accion.Equals("saltarIzq")) || (accion.Equals("saltar")))
            {
                Out = -0.2f;
            }
            else Out = -0.1f;
            return Out;
        }
        // --- funcion evalua cada accion, estado y recompensa. Retorna la accion a tomar
        private String funcionQ(String aA, Bloque eA, float rA) // valores Actuales : accionActual, estadoActual, recompensaActual
        {
            TablaAER Q = new TablaAER();
            String accion = "empty";
            // guardar
            // XML.Serialize(Q, "memoria");
            // cargar
            // XML.Deserialize<TablaAER>("memoria");
            // verificamos si existe el archivo memoria
            if (File.Exists("memoria"))
            {
                // cargar memoria
                Q = XML.Deserialize<TablaAER>("memoria");
                if (Q.getTupla(aA,eA) != null) // busca la tupla en la tabla
                {
                    ColumnasAER tupla = Q.getActionMaxQ(aA, eA, rA);
                    // agrega la tupla en la tabla
                    Q.addTupla(tupla);
                    accion = tupla.accion;
                    Debug.WriteLine("La tupla si esta");
                }
                else // si no la encuentra
                {
                    ColumnasAER tupla = new ColumnasAER(aA, eA, 0, rA);
                    // agrega la tupla directo en la tabla
                    Q.addTupla(tupla);
                    accion = tupla.accion;
                    Debug.WriteLine("La tupla no esta");
                }
                XML.Serialize(Q, "memoria");
                return accion;
            }
            // algoritmo que se ejecuta por defecto cuando no existe memoria
            else
            {
                // crear informacion
                Q.FilasAER = new List<ColumnasAER>();
                ColumnasAER tupla = new ColumnasAER(aA,eA,0,rA);
                Q.FilasAER.Add(tupla);
                // se agrega el moviemiento a su memoria
                Q.Movimientos = new List<String>();
                Q.Movimientos.Add(aA);
                // guardar informacion en memoria
                XML.Serialize(Q, "memoria");
                // cerrar archivo
                // asigna la accion actual como accion por defecto
                accion = aA;
                // retorna la accion
                Debug.WriteLine("No existe memoria");
                return accion;
            }
        }
        // --- disminuye el area de hxh a 3x3
        private Bloque disminuirArea()
        {
            Vector2 centro = new Vector2(profundidad, profundidad); // centro del area
            Bloque Out = new Bloque();
            List<Sector> swap = new List<Sector>();
            int h = 1; // profundidad
            int m = 0, p = 0;
            for (int y = -h; y < h + 1; y++)
            {
                for (int x = -h; x < h + 1; x++)
                {
                    Sector aux = new Sector();
                    aux.name = "empty";
                    aux.value = false;
                    aux.posicion = new Vector2(p, m);
                    swap.Add(aux);
                    p++;
                }
                m++; p = 0;
            }
            int progresion = 0; // 0 - 9
            foreach (var i in area.sector)
            {
                for (int j = 0; j < 3; j++)
                {
                    for (int k = 0; k < 3; k++)
                    {
                        if ((profundidad + (j - 1) == (i.posicion.X)) && (profundidad + (k - 1) == (i.posicion.Y)))
                        {
                            swap[progresion].name = i.name;
                            swap[progresion].value = i.value;
                            progresion++;
                        }
                    }
                }
            }

            Out.sector = swap;
            
            return Out;
        }

        public acciones Camino(Bloque area)
        {
            BusquedaAestrella cOp = new BusquedaAestrella(area, profundidad); // instancia el algoritmo A*
            Vector2 final = cOp.getPosicionPlayer; // obtiene la posicion del player
            acciones camino = new acciones();
            camino.accion = new List<string>();
            /* 
             *  verifica que el jugador haya cambiado de posicion.
            */
            if (final!=posPlayer)
            {
                Vector2 reff = cOp.getPosicionAgent; // obtiene la posicion del agente (posicion inicial)
                List<Vector2> posiciones = cOp.encontrarCamino();
                foreach (var item in posiciones)
                {
                    //Debug.WriteLine(item + " - " + reff);
                    //derecha
                    if ((item.X == reff.X + 1) && (item.Y == reff.Y))
                    {
                        camino.accion.Add("avanzarDer");
                    }
                    //izquierda
                    if ((item.X == reff.X - 1) && (item.Y == reff.Y))
                    {
                        camino.accion.Add("avanzarIzq");
                    }
                    //arriba
                    if ((item.X == reff.X) && (item.Y == reff.Y - 1))
                    {
                        camino.accion.Add("saltar");
                    }
                    //abajo
                    if ((item.X == reff.X) && (item.Y == reff.Y + 1))
                    {
                        // nose
                    }
                    //derecha-arriba
                    if ((item.X == reff.X + 1) && (item.Y == reff.Y - 1))
                    {
                        camino.accion.Add("saltarDer");
                    }
                    //derecha-abajo
                    if ((item.X == reff.X + 1) && (item.Y == reff.Y + 1))
                    {
                        camino.accion.Add("avanzarDer");
                    }
                    //izquierda-arriba
                    if ((item.X == reff.X - 1) && (item.Y == reff.Y - 1))
                    {
                        camino.accion.Add("saltarIzq");
                    }
                    //izquierda abajo
                    if ((item.X == reff.X - 1) && (item.Y == reff.Y + 1))
                    {
                        camino.accion.Add("avanzarIzq");
                    }
                    reff = item;
                }
                posPlayer = final;
                return camino;
            }
            
            return camino;
        }

        public String CaminoActualizado(Bloque area) // retorna 1 accion
        {
            BusquedaAestrella cOp = new BusquedaAestrella(area, profundidad); // instancia el algoritmo A*
            Vector2 final = cOp.getPosicionPlayer; // obtiene la posicion del player
            Vector2 inicio = cOp.getPosicionAgent; // obtiene la posicion del agente (posicion inicial)
            String Accion = "empty";
            Vector2 sucesion; // item: posicion a alcansar

            if ((final != posPlayer) || (inicio != posAgent)) // si la posicion del jugador cambia
            {
                camino = cOp.encontrarCamino();
                sucesion = camino[1];
            }
            else
            {
                sucesion = camino[1];
            }

            //Debug.WriteLine(item + " - " + reff);
            //derecha
            if ((sucesion.X == inicio.X + 1) && (sucesion.Y == inicio.Y))
            {
                Accion = "avanzarDer";
            }
            //izquierda
            if ((sucesion.X == inicio.X - 1) && (sucesion.Y == inicio.Y))
            {
                Accion = "avanzarIzq";
            }
            //arriba
            if ((sucesion.X == inicio.X) && (sucesion.Y == inicio.Y - 1))
            {
                Accion = "saltar";
            }
            //abajo
            if ((sucesion.X == inicio.X) && (sucesion.Y == inicio.Y + 1))
            {
                // nose
            }
            //derecha-arriba
            if ((sucesion.X == inicio.X + 1) && (sucesion.Y == inicio.Y - 1) )//&& (area.obtenerSector(new Vector2(inicio.X + 1, inicio.Y)).value == true))
            {
                Accion = "saltarDer";
            }
            //derecha-abajo
            if ((sucesion.X == inicio.X + 1) && (sucesion.Y == inicio.Y + 1))
            {
                Accion = "avanzarDer";
            }
            //izquierda-arriba
            if ((sucesion.X == inicio.X - 1) && (sucesion.Y == inicio.Y - 1) )//&& (area.obtenerSector(new Vector2(inicio.X - 1, inicio.Y)).value == true))
            {
                Accion = "saltarIzq";
            }
            //izquierda abajo
            if ((sucesion.X == inicio.X - 1) && (sucesion.Y == inicio.Y + 1))
            {
                Accion = "avanzarIzq";
            }
            //reff = item;
            // asigna los valores de las posiciones al agente y al player
            posPlayer = final;
            posAgent = inicio;

            return Accion;
        }
        // --- funcion que realiza las acciones
        private void Comportamiento(String a)
        {
            //Debug.WriteLine(" -" +a);
            if (a.Equals("avanzarIzq")) avanzarIzquierda();
            else if (a.Equals("avanzarDer")) avanzarDerecha();
            else if (a.Equals("saltarIzq")) { avanzarIzquierda(); saltar(); }
            else if (a.Equals("saltarDer")) { avanzarDerecha(); saltar(); }
            else if (a.Equals("saltar")) saltar();

        }
        // --- funcion que realiza las acciones desde un conjunto de estas
        public override void Comportamiento(acciones a)
        {
            foreach (var accion in a.accion)
            {
                //Debug.WriteLine(accion);
                if (accion.Equals("avanzarIzq")) avanzarIzquierda();
                if (accion.Equals("avanzarDer")) avanzarDerecha();
                if (accion.Equals("saltarIzq")) { avanzarIzquierda(); saltar(); }
                if (accion.Equals("saltarDer")) { avanzarDerecha(); saltar(); }
                if (accion.Equals("saltar")) saltar();
            }
        }
        // --- fin ---
    }
}