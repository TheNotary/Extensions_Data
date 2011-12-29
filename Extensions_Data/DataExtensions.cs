﻿using System;
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
            DataSet Result = mainDataSet.Copy();
            if (subtractorDataSet.Tables.Count < 1)
                return Result;

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






    }
}