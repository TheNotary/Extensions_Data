using System;
using System.Collections.Generic;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using System.Data.Common;



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
        /// <returns>If subtractorDataSet is null, this function just returns mainDataSet...</returns>
        public static DataSet Minus(this DataSet mainDataSet, DataSet subtractorDataSet)
        {
            #region Declare variables
            DataSet MainDataSet = mainDataSet.Copy();

            string formatterText = ""; // this is used to format the svl query on the dataset    eg "{0} = {1}"
            string[] myColumnNamesAndVals = new string[subtractorDataSet.Tables[0].Columns.Count * 2];  // This needs to be refactored
            
            Dictionary<string, string> columnValuePairs = new Dictionary<string, string>();


            #endregion


            #region Check that function can run
            if (subtractorDataSet.Tables.Count < 1)
                return MainDataSet;

            // if the two DataSets aren't comparable (ie columns don't match) then throw exception
            if (mainDataSet.Tables[0].Columns.Count != subtractorDataSet.Tables[0].Columns.Count)
                throw new Exception("You attempted to compare two datasets whose first tables had a different number of columns in it. \n\nDataSet.Minus(DataSet)");

            for (int i = 0;  i < mainDataSet.Tables[0].Columns.Count; i++)  // check that each columns name and dataset are equal
            {
                if (mainDataSet.Tables[0].Columns[i].ColumnName != subtractorDataSet.Tables[0].Columns[i].ColumnName)
                    throw new Exception("You attempted to compare two datasets whose first tables had differing column Names...  \n They must appear in the same order... \n\nDataSet.Minus(DataSet)");
                if (mainDataSet.Tables[0].Columns[i].DataType != subtractorDataSet.Tables[0].Columns[i].DataType)
                    throw new Exception("You attempted to compare two datasets whose first tables had differing column datatypes. \n\nDataSet.Minus(DataSet)");
            }
            #endregion

            //myColumnNamesAndVals = setColumnNamesForDataArray(myColumnNamesAndVals, subtractorDataSet.Tables[0].Columns);

            for (int i = 0; i < subtractorDataSet.Tables[0].Columns.Count * 2; i = i + 2)  // Set the Column names for each column (but not their values)
            {
                myColumnNamesAndVals[i] = subtractorDataSet.Tables[0].Columns[i / 2].ColumnName;

                // " {0} = {1} AND {2} = {3} AND {4} = {5} AND {6} = {7} AND {8} = {9} AND {10} = {11} AND {12} = {13} AND {14} = {15} AND {16} = {17} AND {18} = {19} AND {20} = {21}"
                formatterText += " {" + i + "} = {" + (i + 1) + "} AND"; // this should be generated based on whether the value is null or not...
                                                                         // so done down below
            }
            formatterText = formatterText.Substring(1, formatterText.Length - 4); // nop off the final " AND"

            int rowCounter = 0;
            foreach (DataRow subtractorRow in subtractorDataSet.Tables[0].Rows)
            {
                rowCounter++;
                DataColumnCollection subtractorColumns = subtractorDataSet.Tables[0].Columns;
                // select query:  index = 1, data = bla
                List<string> nullDateOrCurrencyColumnNames = new List<string>();
                bool doNulls = false;

                for (int i = 0; i < subtractorColumns.Count; i++)
                {
                    string rowsType = subtractorColumns[i].DataType.Name.ToLower();
                    bool skip = false;

                    string rowsDataAsString = "";
                    //if (((int)subtractorRow[0]) == 238967 && myColumnNamesAndVals[(i * 2)] == "Employee")
                    //    rowsDataAsString = "";

                    #region Format SQL string based on datatype  (SWITCH CASE)
                    switch (rowsType)   // Break at:  subtractorDataSet.Tables[0].Columns[0] == 238967
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
                            if (rowsDataAsString == "")
                            {
                                myColumnNamesAndVals[(i * 2)] = myColumnNamesAndVals[((i - 1) * 2)];
                                myColumnNamesAndVals[(i * 2) + 1] = myColumnNamesAndVals[((i - 1) * 2) + 1];
                                skip = true;

                                doNulls = true;
                                nullDateOrCurrencyColumnNames.Add(subtractorDataSet.Tables[0].Columns[i].ColumnName);
                            }
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
                            if (subtractorRow[i].GetType().Name == "DBNull")
                            {
                                //   "JobNotes" = "Customer"
                                //   null      =  "588"
                                myColumnNamesAndVals[(i * 2)]     = myColumnNamesAndVals[((i - 1) * 2)];
                                myColumnNamesAndVals[(i * 2) + 1] = myColumnNamesAndVals[((i - 1) * 2) + 1];
                                skip = true;

                                doNulls = true;
                                nullDateOrCurrencyColumnNames.Add(subtractorDataSet.Tables[0].Columns[i].ColumnName);
                            }
                            else
                                rowsDataAsString = "'" + ((string)subtractorRow[i]).EscapeSingleQuotes() + "'";
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
                    #endregion

                    if (!skip)
                        myColumnNamesAndVals[(i * 2) + 1] = rowsDataAsString;
                }


                string svl = string.Format(formatterText,   //  eg  "{0} = {1}, {2} = {3}"
                    myColumnNamesAndVals);                  //  eg  { "Index", "1", "StringData", "'bla'" }

                DataRow[] HitRows = MainDataSet.Tables[0].Select(svl);  // Look in MainDataset to see if the subtractor's record exists there
                // and if it does, delete it from the MainDataset

                #region delete all rows that were found in the subtractor row aswell as in the MainDataSet
                foreach (DataRow rw in HitRows)                                     // Check each row of the hits...
                {
                    // I could check for nulls here manually, but that might be a sloppy method
                    if (doNulls)
                        foreach (string nullDate in nullDateOrCurrencyColumnNames)  // Scan through all the null columns and...
                        {
                            if (rw[nullDate].GetType().Name == "DBNull")            // if they're actually DBNull... then... delete them from hits?
                            {
                                rw.Delete();  // this doesn't work because it deletes it at the first Null occurance, instead of 100% null match
                                break;
                            }
                        }
                    else
                        rw.Delete();
                }
                #endregion
                // reset the array incase we messed with it in our "optimized" way...
                myColumnNamesAndVals = setColumnNamesForDataArray(myColumnNamesAndVals, subtractorDataSet.Tables[0].Columns); // OPTIMIZE bury this behind a bool check or get rid of all these damn bool checks
            }
            MainDataSet.AcceptChanges();

            return MainDataSet;
        }


        private static string[] setColumnNamesForDataArray(string[] myColumnNamesAndVals, DataColumnCollection subtractorColumns)
        {
            for (int i = 0; i < subtractorColumns.Count * 2; i = i + 2)  // Set the Column names for each column (but not their values)
            {
                myColumnNamesAndVals[i] = subtractorColumns[i / 2].ColumnName;
            }
            return myColumnNamesAndVals;
        }





        /// <summary>
        /// This function returns the column names of the first table in a DataSet
        /// </summary>
        /// <param name="ds"></param>
        /// <returns></returns>
        public static string[] GetTableHeadersAsStrings(this DataSet ds)
        {
            string[] headers = new string[ds.Tables[0].Columns.Count];
            int i = 0;
            foreach (DataColumn col in ds.Tables[0].Columns)
            {
                headers[i++] = col.ColumnName;
            }
            return headers;
        }

        /// <summary>
        /// Get's the table headers for the table name specified
        /// </summary>
        /// <param name="ds"></param>
        /// <param name="tableName">Table you would like to query</param>
        /// <returns></returns>
        public static string[] GetTableHeadersAsStrings(this DataSet ds, string tableName)
        {
            DataTable dt;
            try
            {
                dt = ds.Tables[tableName];

                string[] headers = new string[dt.Columns.Count];
                int i = 0;
                foreach (DataColumn col in dt.Columns)
                {
                    headers[i++] = col.ColumnName;
                }
                return headers;
            }
            catch (Exception e)
            {
                throw new Exception("Error...  probably table name not found or something..." + e.Message);
            }
            
        }
        


        /// <summary>
        /// This checks if a table exists in a database. 
        /// 
        /// Creds to baavgai and Nakor on this very sly extension method
        /// </summary>
        /// <param name="conn"></param>
        /// <param name="tableName"></param>
        /// <param name="owner"></param>
        /// <param name="database"></param>
        /// <returns></returns>
        public static bool TableExists(DbConnection conn, string tableName, string owner, string database)
        {
            DataTable dt;
            try
            {
                conn.Open();
                dt = conn.GetSchema("Tables", new string[] { database, owner, tableName });
            }
            finally
            {
                conn.Close();
            }
            return (dt == null) ? false : dt.Rows.Count > 0;
        }

        public static bool TableExists(DbConnection conn, string tableName)
        {
            return TableExists(conn, tableName, null, null);
        }


        /// <summary>
        /// This checks if a table exists in a database.  
        /// 
        /// Note, you must add a reference to System.Data.Common to your project
        /// </summary>
        /// <param name="myAdapter"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public static bool TableExists(this DbDataAdapter myAdapter, string tableName)
        {
            return TableExists(myAdapter.SelectCommand.Connection, tableName);
        }

        

        /// <summary>
        /// This allows me to convert an array of DataRows into a DataSet.
        /// </summary>
        /// <param name="drs"></param>
        /// <returns></returns>
        public static DataSet ToDataSet(this DataRow[] drs)
        {
            throw new Exception("Not implimented yet cause I thought I needed it... but I don't...");
        }




        /// <summary>
        /// This function will convert the table to a CSV file
        /// where the values are safely encapsulated in quotes, 
        /// and quotes appearing in the values are escaped by pairing them 
        /// </summary>
        /// <param name="dsToConvert"></param>
        /// <param name="pathToCSVToMake"></param>
        /// <returns></returns>
        public static string ToCSV(this DataTable table)
        {
            int lastColumnIndex = table.Columns.Count - 1;

            var result = new StringBuilder();
            for (int i = 0; i < table.Columns.Count; i++)
            {
                result.Append("\"" + table.Columns[i].ColumnName.EscapeQuotes() + "\"");
                result.Append(i == lastColumnIndex ? "\n" : ",");
            }

            foreach (DataRow row in table.Rows)
            {
                for (int i = 0; i < table.Columns.Count; i++)
                {
                    result.Append("\"" + row[i].ToString().EscapeQuotes() + "\"");
                    result.Append(i == lastColumnIndex ? "\n" : ",");
                }
            }

            return result.ToString();
        }

        public static string EscapeQuotes(this string sourceString)  //      "This is kemp"s day off
        {
            //string test1 = "\"This is kemp\"s day off";
            string SourceString = sourceString;

            //int countTill = sourceString.Length*2;
            int i = 0;
            while (true)
            {
                int pos = SourceString.IndexOf('"', i);
                if (pos == -1)
                    break;
                SourceString = SourceString.Insert(pos, "\"");
                i = pos + 2;
            }

            return SourceString;
        }

        public static string EscapeSingleQuotes(this string sourceString)  //   "This is kemp's day off"   =>  "This is kemp''s day off"
        {
            string SourceString = sourceString;

            int i = 0;
            while (true)
            {
                int pos = SourceString.IndexOf("'", i);
                if (pos == -1)
                    break;
                SourceString = SourceString.Insert(pos, "'");
                i = pos + 2;
            }

            return SourceString;
        }





    }
}
