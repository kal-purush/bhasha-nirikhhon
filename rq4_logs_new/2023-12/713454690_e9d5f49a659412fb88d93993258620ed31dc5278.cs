/*
    
    Matteo Angiolillo, 3°H, 2023-12-19 terminato a casa il 2023-12-22

    Esercizio in classe: simulazione di un gioco: Mastermind

    Il giocatore deve indovinare un numero di 4 cifre [0-9], non ripetitive, in un numero di tentativi stabilito da noi in base alla difficoltà, facile, media, difficile 
    
 */

using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Schema;

namespace MasterMindGioco
{
    
    internal class Program
    {
        #region Variabili globali

        static Random rnd = new Random(); // oggetto per i numeri random
        static int tentativiPartita = 0; // tentativi partita

        #endregion

        #region Funzione per la Cornice e intestazione buildFrame();

        static void buildFrame()
        {
            // funzione per la cornice
            Console.Title = "Mastermind - Matteo Angiolillo | 3°H | 2023-12-19";
            Console.WriteLine("---------------------------------------------------\n" +
                              "     | Matteo Angiolillo | 3°H | 2023-12-19 |      \n" +
                              "             Benvenuto in Mastermind!               \n" +
                              "---------------------------------------------------");
        }

        #endregion

        #region Generazione array di numeri da indovinare generaNumero();

        static int[] generaNumero()
        {
            List<int> urna = new List<int>(); // lista con i numeri da estrarre
            int[] codiceDaIndov = new int[4]; // array con i numeri da indovinare

            for (int i = 0; i<10; i++) // riempio la lista di numeri da 0 a 9
            {
                urna.Add(i); 
            }
            
            for (int i = 0; i<4; i++)
            {
                int posiziioneN = rnd.Next(0, urna.Count); // posizione dell'urna del numero che toglierò

                int numeroGenerato = urna[posiziioneN]; // mi salvo il numero corrispondente alla posizione

                urna.RemoveAt(posiziioneN); // tolgo il numero generato dalla lista per evitare la ripetizione

                codiceDaIndov[i] = numeroGenerato; // salvo il numero generato nel codice finale

            }

            return codiceDaIndov;
        }

        #endregion

        #region lettura difficoltà e assegnazione tentativi sceltaDifficoltà();

        static void sceltaDifficoltà()
        {
            bool inputOk = true;

            Console.Write(" Selezionare la difficoltà del gioco -> \n" +
                              "[f] = Facile      -> 9 tentativi\n" +
                              "[m] = Medio       -> 5 tentativi\n" +
                              "[d] = Difficile   -> 3 tentativi\n" +
                              "[i] = Impossibile -> 1 tentativo\n");
                              
            do // ciclo per controllare validità dell'input cicla finchè l'input non è validato
            {
                Console.Write("-> ");

                string sceltaUser = Console.ReadLine().ToUpper(); // .ToUpper() è per evitare confusioni maiusc e minusc

                inputOk = true;

                switch (sceltaUser) // switch che gestisce tutti i casi
                {
                    case "F":
                        tentativiPartita = 9;
                        break;

                    case "M":
                        tentativiPartita = 5;
                        break;

                    case "D":                        
                        tentativiPartita = 3;
                        break;

                    case "I":
                        tentativiPartita = 1;
                        break;

                    default: // caso di un tasto non di quelli indicati

                        Console.WriteLine("\n ...Inserire solo i caratteri mostrati...riprovare");
                        inputOk = false;
                        break;
                }

            } while (!inputOk);
        }

        #endregion

        #region Controllo validità array di input InsertAndCheckInput();

        static int[] InsertAndCheckInput()
        {
            int[] finalGuess = new int[4]; // array del codice finale da controllare
            bool inputOk = true;

            do // ciclo per controllare la validità dell'input
            {

                Console.Write("\n Inserisci il codice -> "); 

                string firstGuess = Console.ReadLine(); // leggo il codice, e lo salvo in una stringa

                if (firstGuess.Length < 4 || firstGuess.Length > 4) // controllo sia della lunghezza giusta, in caso di errore, richiede l'input
                {
                    Console.ForegroundColor = ConsoleColor.Red;
                    Console.WriteLine("\n errore...il codice deve essere lungo 4 cifre...");
                    Console.ForegroundColor = ConsoleColor.White;
                    inputOk = false;
                    continue;
                }

                for (int i = 0; i < 4; i++) // ciclo che converte la stringa in int[] e controlla che le cifre siano tutte numeri, e diverse
                {
                    inputOk = int.TryParse(firstGuess[i].ToString(), out finalGuess[i]);

                    if (!inputOk) // controlla che non ci siano caratteri e solo cifre
                    {   
                        Console.WriteLine(" ERRORE...Inserire solo numeri...riprova");
                        inputOk = false;
                        break;
                    }
                    if (i>0) // serve per gestire l'eccezione che se partiamo dal primo carattere allora l'indice prima non esiste
                    {

                        if (firstGuess[i] == firstGuess[i - 1])
                        {
                            Console.WriteLine(" ERRORE...Inserire solo numeri DIVERSI...riprova");
                            inputOk = false;
                            break;
                        }                        
                    }
                }                

            } while (!inputOk); // cicla fino a che l'input non sia valido
             
            return finalGuess; // ritorna l'array col codice finale
        }

        #endregion

        #region controllo effettivo del gioco indovinaCodice();

        static void indovinaCodice() // controlla che il codice di user sia uguale a quello generato
        {

            int[] codiceDaIndovinare = generaNumero();

            for (int i = 0; i<4; i++) // stampa del numero da indovinare a scopo di debug :)
            {
                Console.Write($"{codiceDaIndovinare[i]} | ");

            }
            Console.WriteLine("\n ------------------------------------");

            bool haiVinto = false; // flag che ci permette di stampare due messaggi diversi alla fine

            while (tentativiPartita > 0) // cicla finchè ho tentativi rimasti
            {

                Console.ForegroundColor = ConsoleColor.DarkCyan;
                Console.WriteLine($"\n - Hai {tentativiPartita} tentativi rimasti -");
                Console.ForegroundColor = ConsoleColor.White;

                int[] codiceUtente = InsertAndCheckInput(); // controlla che l'input sia valido -> senza ripetizioni e lungo 4 cifre, e solo di numeri                              

                int countPosition = 0; // numero di valori uguali tra gli array allo stesso indice
                int countJustNumber = 0; // numero di valori uguali tra gli array NON allo stesso indice


                for (int i = 0; i < 4; i++)
                {
                    if (codiceUtente[i] == codiceDaIndovinare[i]) // se il valore all'indice i, è uguale al valore dell'altro array allo stesso indice 
                    {
                        countPosition++; // allora vuol dire che i due numeri sono esatti e sono alla stessa posizione
                    }
                    
                    // ora mi salvo il valore N, e per 4 volte controllo sia nell'altro ciclo

                    int numeroTemp = codiceUtente[i]; // mi tengo il primo valore nell'array

                    for (int j = 0; j<4; j++) // questo ciclo serve appunto per confrontare il numeroTemp con tutti gli altri valori del codiceDaIndovinare
                    {
                        if (numeroTemp == codiceDaIndovinare[j]) // e ogni volta controllo se il numero è presente nella posizione J, quindi in qualsiasi posizione
                        {
                            countJustNumber++; // in caso lo sia aumento il mio contatore
                        }
                    }
                }
                
                if (countPosition == 4) // se tutti e quattro i numeri sono giusti
                {
                    Console.ForegroundColor = ConsoleColor.Green;
                    Console.WriteLine("\n  --- Hai vinto!! --- ");
                    Console.ForegroundColor = ConsoleColor.White;
                    haiVinto = true;
                    break;
                }

                else // altrimenti do le stats del tentativo
                {
                    countJustNumber -= countPosition;
                    Console.ForegroundColor = ConsoleColor.DarkYellow;
                    Console.WriteLine($"Ci sono {countJustNumber} numeri giusti, ma alla posizione sbagliata");
                    Console.ForegroundColor = ConsoleColor.Cyan;
                    Console.WriteLine($"{countPosition} numeri sono giusti e alla posizione giusta");
                    Console.ForegroundColor = ConsoleColor.White;

                }
                tentativiPartita--;

            }

            if (!haiVinto) // stampa il messaggio del caso in cui si ha perso
            {
                Console.ForegroundColor = ConsoleColor.DarkRed;
                Console.WriteLine("----------------------------");
                Console.WriteLine("\n --- Hai perso :( --- \n");
                Console.WriteLine("----------------------------");
                Console.ForegroundColor = ConsoleColor.White;
            }
            
        }

        #endregion

        static void Main(string[] args)
        {
            
            buildFrame();

            sceltaDifficoltà();

            indovinaCodice();

            Console.WriteLine("\n -------------------------------------------------------------- \n Premi un tasto per chiudere la finestra...");
            Console.ReadKey();

        }

    }
}