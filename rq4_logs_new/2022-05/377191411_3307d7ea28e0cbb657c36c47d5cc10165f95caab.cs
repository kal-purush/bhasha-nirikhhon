using RadiantReader.Configuration;
using RadiantReader.DataBase;

namespace RadiantReader.Managers
{
    public static class StateManager
    {
        // ********************************************************************
        //                            Public
        // ********************************************************************
        public static void SetCurrentBook(RadiantReaderBookDefinitionModel aBookDefinition)
        {
            var _Config = RadiantReaderConfigurationManager.ReloadConfig();

            _Config.State.SelectedBook ??= new SelectedBookState();

            _Config.State.SelectedBook.BookDefinitionId = aBookDefinition.BookDefinitionId;
            _Config.State.SelectedBook.BookChapterIndex = 0;

            RadiantReaderConfigurationManager.SetConfigInMemory(_Config);
            RadiantReaderConfigurationManager.SaveConfigInMemoryToDisk();
        }
    }
}