using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Text;
using System.Windows.Forms;
using Extensions_Data;
using System.Data.OleDb;
using MySql.Data.MySqlClient;


namespace DemonstrationProject
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }

        private void button1_Click(object sender, EventArgs e)
        {
            // do A.Minus(B) here...
            string source = "\"this is kemp\"s day off";

            source = source.EscapeQuotes();

            MessageBox.Show(source);
        }

        private void btnTestTableExists_Click(object sender, EventArgs e)
        {
            string tableName = "MyTable";
            string cmdText = "select * from " + tableName;

            // MySql Assumes 
            // Access DB Assumes c:/test.mdb exists and may or maynot have a table named "mytable"
            // MYSQL CONNECTION
            string MySqlConString = "SERVER=localhost;DATABASE=test;UID=test1;PASSWORD=testpass;";
            MySqlConnection mysqlConn = new MySqlConnection(MySqlConString);
            MySqlCommand mySqlCommand = mysqlConn.CreateCommand();

            mySqlCommand.CommandText = cmdText;
            MySqlDataAdapter mySqlAdapter = new MySqlDataAdapter(mySqlCommand.CommandText, mysqlConn);

            // OLEDB CONNECTION
            string oleConString = "Provider=Microsoft.Jet.OLEDB.4.0;Data Source = " + @"c:/test.mdb" + ";";
            OleDbConnection oleConn = new OleDbConnection(oleConString);
            OleDbCommand oleCommand = new OleDbCommand(cmdText);

            OleDbDataAdapter myOleAdapter = new OleDbDataAdapter(oleCommand.CommandText, oleConn);


            //  ACTUAL TESTING
            bool mySqltableExist = Extensions_Data.Data_Extensions.TableExists(mysqlConn, "mytable777");
            bool oleTableExist = Extensions_Data.Data_Extensions.TableExists(oleConn, "mytable777");

            mySqltableExist = mySqlAdapter.TableExists("mytable");
            oleTableExist = myOleAdapter.TableExists("mytable");


            // scratch stuff
            MessageBox.Show("hey, it worked (T/F): " + mySqltableExist.ToString());

        }
    }
}
