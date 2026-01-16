
namespace Modulewijzer.DataAccess
{
    public sealed class DbConnection
    {
        #region Singleton code.
        private static readonly DbConnection instance = new DbConnection();


        /// <summary>
        /// Gets the connection's instance.
        /// </summary>
        public static DbConnection Instance => instance;
        #endregion


        public const string ConnectionString = "Data Source=(LocalDB)\\MSSQLLocalDB;AttachDbFilename=|DataDirectory|Modulewijzer.mdf;Integrated Security=True;Connect Timeout=30";


        public TabelModule Module { get; } = new TabelModule();
        public TabelCompetentie Competentie { get; } = new TabelCompetentie();
        public TabelDocent Docenten { get; } = new TabelDocent();
    }
}