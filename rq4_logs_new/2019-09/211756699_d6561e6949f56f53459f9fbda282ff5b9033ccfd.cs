using System;
using QueMePongo;

namespace queMePongo.Repositories
{
    public class AtuendoRepository
    {
        public void Insert(Atuendo atuendo, DB context)
        {
            context.atuendos.Add(atuendo);
            context.SaveChanges();
            Console.WriteLine($"\nAtuendo {atuendo} creado!");
        }

        public void Update(Atuendo atuendo, DB context)
        {

        }

        public void Delete(int atuendoId)
        {

        }
    }
}