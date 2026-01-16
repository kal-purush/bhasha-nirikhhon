using MediatR;
using Rems.Application.CQRS;
using System;
using System.Collections.Generic;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Threading.Tasks;

namespace WindowsClient.Models
{
    public interface INodeValidater
    {
        event Action<string, object> StateChanged;

        event Action<IEnumerable<RichText>> SetAdvice;

        void Validate();
    }

    public class NullValidater : INodeValidater
    {
        public event Action<string, object> StateChanged;
        public event Action<IEnumerable<RichText>> SetAdvice;

        public void Validate()
        {
            StateChanged?.Invoke("Valid", true);

            var text = new RichText[]
            {
                new RichText
                { Text = "Ready to be imported.\n", Color = Color.Black }
            };

            SetAdvice?.Invoke(text);
        }
    }

    #region Table validation

    public class TableValidater : INodeValidater
    {
        public event Action<string, object> StateChanged;
        public event Action<IEnumerable<RichText>> SetAdvice;

        readonly DataTable table;

        public TableValidater(DataTable table)
        {
            this.table = table;
        }

        public void Validate()
        {
            var valid = table.Columns
                .Cast<DataColumn>()
                .Select(c => (bool)c.ExtendedProperties["Valid"])
                .Aggregate((v1, v2) => v1 &= v2);

            if (valid)
            {
                StateChanged?.Invoke("Valid", true);

                var advice = new RichText[]
                {
                    new RichText
                    { 
                        Text = "This table is valid. Check the other tables prior to import.", 
                        Color = Color.Black
                    }
                };
                SetAdvice?.Invoke(advice);
            }
            else
            {
                StateChanged?.Invoke("Override", "Warning");

                var advice = new RichText[]
                {
                    new RichText
                    {
                        Text = "This table contains columns that REMS does not recognise. "
                            + "Please fix the columns before importing",
                        Color = Color.Black
                    }
                };
                SetAdvice?.Invoke(advice);
            }
        }
    }

    public class CustomTableValidater : INodeValidater
    {
        public event Action<IEnumerable<RichText>> SetAdvice;

        public event Action<string, object> StateChanged;

        public readonly string[] columns;

        protected readonly DataTable table;

        public CustomTableValidater(DataTable table, string[] columns)
        {
            this.table = table;
            this.columns = columns;
        }

        public void Validate()
        {
            bool valid = true;

            foreach (DataColumn col in table.Columns)
                valid &= (bool)col.ExtendedProperties["Valid"];

            if (valid)
            {
                StateChanged?.Invoke("Valid", true);

                var advice = new RichText[]
                {
                    new RichText
                    {
                        Text = "This table is valid. Check the other tables prior to import.",
                        Color = Color.Black
                    }
                };
                SetAdvice?.Invoke(advice);
            }
            else
            {
                var advice = new List<RichText>
                {
                    new RichText
                    { Text = "This table contains mismatched columns. \n\n" +
                    "To enable import, please verify that the highlighted (!) columns contain the expected data. \n\n" +
                    "If necessary, ignore any (X) columns.", Color = Color.Black }
                };

                SetAdvice?.Invoke(advice);
                StateChanged?.Invoke("Override", "Warning");
            }
                
        }
    }

    #endregion

    #region Column validation

    public class ColumnValidater : INodeValidater
    {
        public event Func<object, Task<object>> Query;
        private async Task<T> InvokeQuery<T>(IRequest<T> query) => (T)await Query(query);

        public event Action<string, object> StateChanged;
        public event Action<IEnumerable<RichText>> SetAdvice;

        readonly DataColumn column;

        public ColumnValidater(DataColumn column)
        {
            this.column = column;
        }

        public async void Validate()
        {
            // If the colum node is not valid for import, update the state to warn the user
            if (column.ExtendedProperties["Info"] is null && !await IsTrait())
                StateChanged?.Invoke("Valid", false);
            else
                StateChanged?.Invoke("Valid", true);
        }

        private async Task<bool> IsTrait()
        {
            return
                    // Test if the trait is in the database
                    await InvokeQuery(new TraitExistsQuery() { Name = column.ColumnName })
                    // Or in the spreadsheet traits table
                    || column.Table.DataSet.Tables["Traits"] is DataTable traits
                    // If it is, find the column of trait names
                    && traits.Columns["Name"] is DataColumn name
                    // 
                    && traits.Rows.Cast<DataRow>().Any(r => r[name].ToString() == column.ColumnName);
        }
    }

    public class OrdinalValidater : INodeValidater
    {
        public event Action<string, object> StateChanged;
        public event Action<IEnumerable<RichText>> SetAdvice;

        readonly DataColumn col;
        readonly int ordinal;
        readonly string name;

        public OrdinalValidater(DataColumn column, int o, string n)
        {
            col = column;
            ordinal = o;
            name = n;
        }

        public void Validate()
        {
            if (col.Ordinal == ordinal && col.ColumnName == name)
            {
                StateChanged?.Invoke("Valid", true);
                StateChanged?.Invoke("Override", "");

                var advice = new RichText[]
                {
                    new RichText
                    { Text = "Ready to be imported.\n", Color = Color.Black }
                };

                SetAdvice?.Invoke(advice);
            }                
            else
            {
                var advice = new RichText[]
                {
                    new RichText
                    { Text = "Column does not match the expected data\n", Color = Color.Black },
                    new RichText
                    { Text = "Expected: " + name + "\n", Color = Color.Black },
                    new RichText
                    { Text = "Detected: " + col.ColumnName + "\n", Color = Color.Black }
                };

                SetAdvice?.Invoke(advice);
                StateChanged?.Invoke("Override", "Warning");
            }
        }
    }

    #endregion
}