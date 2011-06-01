using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using MySql.Data.MySqlClient;



// this fixes extension methods in .NET 2.0
namespace System.Runtime.CompilerServices
{
    [AttributeUsage(AttributeTargets.Method, AllowMultiple = false, Inherited = false)]
    public class ExtensionAttribute : Attribute
    {
    }
}


namespace Extensions_Data
{
    public static class Data_Extensions
    {

        /// <summary>
        /// Consider  NR = NewerDataset.Minus(oldDataSet)
        /// NR will be any new records that weren't present in oldDataSet, but exist in NewerDataset
        /// 
        /// Consider DR = oldDataSet.Minus(NewerDataset)
        /// DR will be any deleted rows that existed in oldDataset but did not appear in NewerDataset.  
        /// </summary>
        /// <param name="mainDataSet"></param>
        /// <param name="subtractorDataSet"></param>
        /// <returns></returns>
        public static DataSet Minus(this DataSet mainDataSet, DataSet subtractorDataSet)
        {
            DataSet Result = mainDataSet.Copy();

            string formatterText = ""; // this is used to format the svl query on the dataset    eg "{0} = {1}"
            string[] myColumnNamesAndVals = new string[subtractorDataSet.Tables[0].Columns.Count * 2];
            for (int i = 0; i < subtractorDataSet.Tables[0].Columns.Count * 2; i = i + 2)
            {
                myColumnNamesAndVals[i] = subtractorDataSet.Tables[0].Columns[i / 2].ColumnName;


                formatterText += " {" + i + "} = {" + (i + 1) + "} AND";
            }
            formatterText = formatterText.Substring(1, formatterText.Length - 4);


            foreach (DataRow subtractorRow in subtractorDataSet.Tables[0].Rows)
            {
                // select query:  index = 1, data = bla
                List<string> nullDateOrCurrencyColumnNames = new List<string>();
                bool doNulls = false;

                for (int i = 0; i < subtractorDataSet.Tables[0].Columns.Count; i++)
                {
                    string rowsType = subtractorDataSet.Tables[0].Columns[i].DataType.Name.ToLower();

                    bool skip = false;

                    string rowsDataAsString = "";

                    switch (rowsType)
                    {
                        case "autoincrement":
                            rowsDataAsString = Convert.ToString(subtractorRow[i]);
                            break;
                        case "datetime":
                            if (subtractorRow[i].GetType().Name == "DBNull")
                            {
                                //throw new Exception("You have a null date field in one of your databases, and I don't know what to do to select a null date filed in a dataset.  Sry =(");
                                // Alt:
                                // 
                                //rowsDataAsString = "\"\"";
                                myColumnNamesAndVals[(i * 2)] = myColumnNamesAndVals[((i - 1) * 2)];
                                myColumnNamesAndVals[(i * 2) + 1] = myColumnNamesAndVals[((i - 1) * 2) + 1];
                                skip = true;

                                doNulls = true;
                                nullDateOrCurrencyColumnNames.Add(subtractorDataSet.Tables[0].Columns[i].ColumnName);
                            }
                            else
                            {
                                DateTime theDate = (DateTime)subtractorRow[i];
                                rowsDataAsString = "#" + theDate.ToShortDateString() + "#";
                            }
                            break;
                        case "int32":
                            rowsDataAsString = Convert.ToString(subtractorRow[i]);
                            break;
                        case "decimal":
                            rowsDataAsString = Convert.ToString(subtractorRow[i]);
                            if (rowsDataAsString == "")
                            {
                                myColumnNamesAndVals[(i * 2)] = myColumnNamesAndVals[((i - 1) * 2)];
                                myColumnNamesAndVals[(i * 2) + 1] = myColumnNamesAndVals[((i - 1) * 2) + 1];
                                skip = true;

                                doNulls = true;
                                nullDateOrCurrencyColumnNames.Add(subtractorDataSet.Tables[0].Columns[i].ColumnName);
                            }
                            break;
                        case "double":
                            rowsDataAsString = Convert.ToString(subtractorRow[i]);
                            break;
                        case "string":
                            rowsDataAsString = "'" + (string)subtractorRow[i] + "'";
                            break;
                        //case "yesno":
                        //        rowsDataAsString = Convert.ToString(subtractorRow[i]);
                        //        break;
                        case "boolean":
                            if (subtractorRow[i].GetType().Name == "DBNull")
                            {
                                break;
                            }
                            rowsDataAsString = Convert.ToString(subtractorRow[i]);
                            break;
                        default:
                            throw new Exception("unexpected datatype.  Add this type to the case switch in the Minus extention.");
                    }

                    if (!skip)
                        myColumnNamesAndVals[(i * 2) + 1] = rowsDataAsString;
                }



                string svl = string.Format(formatterText,   //  eg  "{0} = {1}, {2} = {3}"
                    myColumnNamesAndVals);                  //  eg  { "Index", "1", "Data", "bla" }

                DataRow[] HitRows = Result.Tables[0].Select(svl);

                foreach (DataRow rw in HitRows)
                {
                    // I could check for nulls here manually, but that might be a sloppy method
                    if (doNulls)
                        foreach (string nullDate in nullDateOrCurrencyColumnNames)
                        {
                            if (rw[nullDate].GetType().Name == "DBNull")
                            {
                                rw.Delete();
                                break;
                            }
                        }
                    else
                        rw.Delete();
                }
            }
            Result.AcceptChanges();

            return Result;
        }



        /// <summary>
        /// This checks if a MySql table exists.  
        /// 
        /// Note, you must add a reference to MySql.Data to your project
        /// </summary>
        /// <param name="myAdapter"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public static bool TableExists(this MySql.Data.MySqlClient.MySqlDataAdapter myAdapter, string tableName)
        {
            // WARNING:  To get this to work, you must add a reference to MySql.Data

            string strCheckTable =
            String.Format(
            "IF OBJECT_ID('{0}', 'U') IS NOT NULL SELECT 'true' ELSE SELECT 'false'",
            tableName);


            myAdapter.SelectCommand.CommandText = strCheckTable;
            myAdapter.SelectCommand.Connection.Open();
            myAdapter.SelectCommand.Connection.Close();

            bool result = Convert.ToBoolean(myAdapter.SelectCommand.ExecuteScalar());

            return result;
        }





    }
}
