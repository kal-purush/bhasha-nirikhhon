using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Charlotte.Tools.Annex
{
	public class DateSpanList
	{
		public const char DELIMITER = ',';
		public const char JOINT = '-';

		private class DateInfo
		{
			public int Day;

			public int Date
			{
				get { return DateToDay.ToDate(this.Day); }
			}

			public static DateInfo Create(string str)
			{
				return Create(int.Parse(str));
			}

			public static DateInfo Create(int date)
			{
				int day = DateToDay.ToDay(date);

				if (DateToDay.ToDate(day) != date)
					throw new ArgumentException(string.Format("”{0}”は日付ではありません。", date));

				return new DateInfo()
				{
					Day = day,
				};
			}
		}

		private class DateSpan
		{
			public DateInfo Min;
			public DateInfo Max;

			public static DateSpan Create(string str)
			{
				return Create(str, str);
			}

			public static DateSpan Create(string str1, string str2)
			{
				DateInfo a = DateInfo.Create(str1);
				DateInfo b = DateInfo.Create(str2);

				if (b.Day < a.Day)
					throw new ArgumentException(string.Format("”{0}”は”{1}”より先の日付です。", str1, str2));

				return new DateSpan()
				{
					Min = a,
					Max = b,
				};
			}

			public string GetString()
			{
				if (this.Min.Day == this.Max.Day)
				{
					return "" + this.Min.Date;
				}
				else
				{
					return this.Min.Date + ("" + JOINT) + this.Max.Date;
				}
			}
		}

		private List<DateSpan> DateSpans = new List<DateSpan>();

		public void Add(string str)
		{
			foreach (string span in str.Split(DELIMITER))
			{
				string[] dates = span.Split(JOINT);

				if (dates.Length == 1)
				{
					this.DateSpans.Add(DateSpan.Create(dates[0]));
				}
				else if (dates.Length == 2)
				{
					this.DateSpans.Add(DateSpan.Create(dates[0], dates[1]));
				}
				else
				{
					throw new ArgumentException("”{0}”は日付又は期間ではありません。", span);
				}
			}
		}

		public void Sort()
		{
			this.DateSpans.Sort((a, b) => a.Min.Day - b.Min.Day);
		}

		public void Join()
		{
			for (int index = 1; index < this.DateSpans.Count; index++)
			{
				DateSpan a = this.DateSpans[index - 1];
				DateSpan b = this.DateSpans[index];

				if (a.Max.Day + 1 == b.Min.Day)
				{
					a.Max.Day = b.Max.Day;
					this.DateSpans.RemoveAt(index);
					index--;
				}
			}
		}

		public void Unjoin()
		{
			List<DateSpan> dateSpansNew = new List<DateSpan>();

			foreach (DateSpan dateSpan in this.DateSpans)
			{
				for (int day = dateSpan.Min.Day; day <= dateSpan.Max.Day; day++)
				{
					dateSpansNew.Add(new DateSpan()
					{
						Min = new DateInfo() { Day = day },
						Max = new DateInfo() { Day = day },
					});
				}
			}
			this.DateSpans = dateSpansNew;
		}

		public string GetString()
		{
			List<string> spans = new List<string>();

			foreach (DateSpan dateSpan in this.DateSpans)
			{
				spans.Add(dateSpan.GetString());
			}
			return string.Join("" + DELIMITER, spans);
		}
	}
}