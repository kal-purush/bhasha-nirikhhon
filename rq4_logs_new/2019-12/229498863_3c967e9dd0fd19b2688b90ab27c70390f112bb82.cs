using SQLite;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FormsMvvm2019
{
    public class AppDatabase
    {
        static readonly Lazy<SQLiteAsyncConnection> lazyInitializer = new Lazy<SQLiteAsyncConnection>(() =>
        {
            return new SQLiteAsyncConnection(Constants.DatabasePath, Constants.Flags);
        });

        static SQLiteAsyncConnection Database => lazyInitializer.Value;
        static bool initialized = false;

        public AppDatabase()
        {
            InitializeAsync().SafeFireAndForget(false);
        }

        private async Task InitializeAsync()
        {
            if (!initialized)
            {
                if (!Database.TableMappings.Any(m => m.MappedType.Name == typeof(PrefItem).Name))
                {
                    await Database.CreateTablesAsync(CreateFlags.None, typeof(PrefItem)).ConfigureAwait(false);
                    initialized = true;
                }
            }
        }

        public Task<int> SaveItemsAsync(List<PrefItem> items)
        {
            return Database.InsertAllAsync(items);
        }

        public Task<List<PrefItem>> GetPrefItemsAsync()
        {
            return Database.Table<PrefItem>().ToListAsync();
        }
    }
}