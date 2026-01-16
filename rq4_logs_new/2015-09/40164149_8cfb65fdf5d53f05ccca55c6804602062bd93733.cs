var reader = db.DataReader("select");
 reader.GetString(reader.GetOrdinal("col_1"))
 reader.GetValue(0).ToString().Trim(); 