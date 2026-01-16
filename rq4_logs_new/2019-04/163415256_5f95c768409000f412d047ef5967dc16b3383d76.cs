using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LibOfTimetableOfClasses
{

	/// <summary>
	/// Корпус
	/// </summary>
	public class MEnclosures : Model
	{
		string _name;
		string _university;
		string _address;
		string _phone;
		string _comment;
		
		public string Name
		{
			get
			{
				return _name;
			}
			set
			{
				_name = value;
			}
		}

		public string University
		{
			get
			{
				return _university;
			}
			set
			{
				_university = value;
			}
		}

		public string Address
		{
			get
			{
				return _address;
			}
			set
			{
				_address = value;
			}
		}
		public string Phone
		{
			get
			{
				return _phone;
			}
			set
			{
				_phone = value;
			}
		}
		public string Comment
		{
			get
			{
				return _comment;
			}
			set
			{
				_comment = value;
			}
		}


		/// <summary>
		/// Создает экземпляр
		/// </summary>
		public MEnclosures(string name,string university,string address,string phone,string comment): base()
		{
			Name= name;
			University = university;
			Address = address;
			Phone= phone;
			Comment= comment;
		}

		public MEnclosures(string name) : base()
		{
			Name = name;
		}
	}
}

﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LibOfTimetableOfClasses
{
	class СEnclosures : DataTable, IController
	{
		public СEnclosures() : base("Корпус")
		{
			DataColumn[] keys = new DataColumn[2];
			DataColumn column = new DataColumn();
			column.DataType = typeof(string);
			column.ColumnName = "Name";
			column.ReadOnly = true;
			this.Columns.Add(column);
			keys[0] = column;
			this.PrimaryKey = keys;

			column = new DataColumn();
			column.DataType = typeof(string);
			column.ColumnName = "University";
			column.ReadOnly = true;
			this.Columns.Add(column);
			keys[1] = column;

			column = new DataColumn();
			column.DataType = typeof(string);
			column.ColumnName = "Address";
			column.ReadOnly = true;
			this.Columns.Add(column);

			column = new DataColumn();
			column.DataType = typeof(int);
			column.ColumnName = "Phone";
			column.ReadOnly = true;
			this.Columns.Add(column);

			column = new DataColumn();
			column.DataType = typeof(string);
			column.ColumnName = "Comment";
			column.ReadOnly = true;
			this.Columns.Add(column);
		}


		public bool Delete(Model model)

		{
			MEnclosures mEnclosures = (MEnclosures)model;
			for (int i = 0; i < this.Rows.Count; i++)
			{
				if ((string)this.Rows[i]["Name"] == mEnclosures.Name && (string)this.Rows[i]["University"] == mEnclosures.University)
				{
					this.Rows[i].Delete();
					return true;
				}
			}
			return false;
		}

		private bool isValidKey(MEnclosures mEnclosures)
		{
			foreach (DataRow row in this.Rows)
			{
				if ((string)row["Name"] == mEnclosures.Name)
					return false;
			}
			return true;
		}

		public bool Insert(Model model)
		{
			MEnclosures mEnclosures = (MEnclosures)model;

			if (isValidKey(mEnclosures))
			{
				try
				{

					DataRow newRow = this.NewRow();
					newRow["Name"] = mEnclosures.Name;
					newRow["University"] = mEnclosures.University;
					newRow["Address"] = mEnclosures.Address;
					newRow["Phone"] = mEnclosures.Phone;
					newRow["Comment"] = mEnclosures.Comment;
					this.Rows.Add(newRow);
					return true;
				}
				catch (Exception ex)
				{
					Debug.WriteLine(ex.Source);
					return false;
				}
			}

			return false;

		}

		public bool Update(Model model)
		{
			MEnclosures mEnclosures = (MEnclosures)model;
			for (int i = 0; i < this.Rows.Count; i++)
			{
					if ((string)this.Rows[i]["Name"] == mEnclosures.Name && (string)this.Rows[i]["University"] == mEnclosures.University)
					try
					{
						DataRow newRow = this.NewRow();
						newRow["Name"] = mEnclosures.Name;
						newRow["University"] = mEnclosures.University;
						newRow["Address"] = mEnclosures.Address;
						newRow["Phone"] = mEnclosures.Phone;
						newRow["Comment"] = mEnclosures.Comment;
						this.Rows.Add(newRow);
						return true;
					}
					catch (Exception ex)
					{
						Debug.WriteLine(ex.Source);
						return false;
					}
			}
			return false;
		}

	}
}