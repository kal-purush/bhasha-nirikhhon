using System.Data.SqlClient;
using System.Data;


namespace Modulewijzer.DataAccess
{
    public sealed class SearchEcs : SearchTerm
    {
        private int m_ecs;


        public override bool SetTerm(string term)
        {
            return int.TryParse(term, out m_ecs);
        }

        public override SqlCommand GetCommand()
        {
            string query = "SELECT [ModulewijzerId], [Naam], [EC], [Studiejaar], [Periode] FROM [Modulewijzer]" +
                "WHERE [EC] = @Ecs";

            var cmd = new SqlCommand(query);
            cmd.Parameters.Add(new SqlParameter("@Ecs", SqlDbType.Int) { Value = m_ecs });

            return cmd;
        }
    }
}