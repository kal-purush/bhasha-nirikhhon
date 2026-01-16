namespace Modulewijzer.Data
{
    // TODO: Better name?
    public sealed class DbModules
    {
        #region Singleton code.
        private static readonly DbModules instance = new DbModules();


        /// <summary>
        /// Returns the database's instance.
        /// </summary>
        public static DbModules Instance => instance;
        #endregion


        public TableModules Modules { get; } = new TableModules();
        public TableCompetences Competences { get; } = new TableCompetences();
        public TableMethods Methods { get; } = new TableMethods();
        public TableTeachers Teachers { get; } = new TableTeachers();
    }
}