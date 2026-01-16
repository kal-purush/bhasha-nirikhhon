using Service;

namespace Entitites
{
    internal class Sorced
    {
        public void TestandoSorced()
        {
            SortedSet<string> setA = new SortedSet<string>();

            setA.Add("a");
            setA.Add("b");
            setA.Add("c");
            setA.Add("c"); //Não Vai!

            RetornoService.RetornarValores(setA);

            SortedSet<string> setB = new SortedSet<string>();

            setB.Add("f");
            setB.Add("d");
            setB.Add("e");
            setB.Add("e"); //Não Vai!


            //Union - Unir duas SortedSet
            SortedSet<string> setC = new SortedSet<string>(setA);
            setC.UnionWith(setB); // Método para unir SortedSet

            //Intercetion - Retorna os dados repetidos
            //setA.IntersectWith(setC);

            //Diference - Retorna os dados diferentes
            //setA.IntersectWith(setC);

            Console.WriteLine("SetC Union");
            RetornoService.RetornarValores(setC);

            //SortedSet<string> nomes = new SortedSet<string>();

            //nomes.Add("Paulo");
            //nomes.Add("Anita"); // Ele organiza nomes!
            //nomes.Add("Caio");

            //RetornoService.RetornarValores(nomes);
        }
    }
}