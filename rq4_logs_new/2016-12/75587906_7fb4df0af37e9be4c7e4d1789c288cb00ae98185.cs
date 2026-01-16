using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.UI;
using System.Web.UI.WebControls;
using System.Data;
using System.Data.Sql;

namespace WoWSimulator
{
    public partial class CharacterInfo : System.Web.UI.Page
    {
        protected void Page_Load(object sender, EventArgs e)
        {
            PopulateDatatables();

        }

        private void PopulateDatatables()
        {
            PopulateCharacterCountLabel();
            PopulateCharacterInfoTable();
            PopulateBattletagLevelTable();
        }

        private void PopulateCharacterCountLabel()
        {
            CharacterCountLabel.Text = "Total Character Count: " + GetCharacterCount();
        }

        private string GetCharacterCount()
        {
            string sqlString = "select count(sys.character.Battletag) from sys.character;";
            return (SQL.RunSQL(sqlString).Rows[0].ItemArray[0].ToString());
        }

        private void PopulateCharacterInfoTable()
        {
            string sqlString = "select sys.character.Battletag, sys.character.name as 'Character', sys.character.race as 'Race', sys.character.Level, sys.character.Realm_Name as 'Realm' from sys.character";
            DataTable CharacterInfoTable = new DataTable();
            CharacterInfoTable = SQL.RunSQL(sqlString);

            //Add extra rows to fill gaps
            AddDummyRows(CharacterInfoTable, 15);

            DataView view = CharacterInfoTable.DefaultView;
            BattletagLookupListview.DataSource = view;
            BattletagLookupListview.DataBind();
        }

        private void PopulateBattletagLevelTable()
        {
            string sqlString = "select sys.game_account.Battletag as Battletag, (select avg(sys.character.Level) from sys.character where sys.character.Battletag=sys.game_account.Battletag) as 'AverageLevel', (select max(sys.character.Level) from sys.character where sys.character.Battletag=sys.game_account.Battletag) as 'MaxLevel' from sys.game_account";

            DataTable BattletagLevelTable = new DataTable();
            BattletagLevelTable = SQL.RunSQL(sqlString);

            //Add extra rows to fill gaps
            AddDummyRows(BattletagLevelTable, 15);

            DataView view = BattletagLevelTable.DefaultView;
            BattletagLevelListview.DataSource = view;
            BattletagLevelListview.DataBind();
        }

        private void AddDummyRows(DataTable table, int minimumRows)
        {
            while (table.Rows.Count < minimumRows)
            {
                DataRow row = table.NewRow();
                table.Rows.Add(row);
            }
        }

        protected void searchButton2_Click(object sender, EventArgs e)
        {

        }

        protected void searchButton_Click(object sender, EventArgs e)
        {

        }
    }
}