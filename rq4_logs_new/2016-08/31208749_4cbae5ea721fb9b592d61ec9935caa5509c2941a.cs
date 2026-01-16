using System;
using RestSharp;

namespace Scalarm
{
	public class GetResultsOptions
	{
		public bool WithIndex { get; set; }
		public bool WithParams { get; set; }
		public bool WithMoes { get; set; }
		public bool WithStatus { get; set; }

		// set to negative number to disable usage of min index
		public int MinIndex { get; set; }

		// set to negative number to disable usage of max index
		public int MaxIndex { get; set; }

		private string boolToQueryValue(bool value)
		{
			return value ? "1" : "0";
		}

		public GetResultsOptions()
		{
			// set default options
			this.WithIndex = false;
			this.WithParams = true;
			this.WithMoes = true;
			this.WithStatus = true;

			this.MinIndex = -1;
			this.MaxIndex = -1;
		}

		public void AddUrlSegments(IRestRequest request)
		{
			request.AddQueryParameter("with_index", boolToQueryValue(this.WithIndex));
			request.AddQueryParameter("with_params", boolToQueryValue(this.WithParams));
			request.AddQueryParameter("with_moes", boolToQueryValue(this.WithMoes));
			request.AddQueryParameter("with_status", boolToQueryValue(this.WithStatus));

			if (this.MinIndex >= 1) {
				request.AddQueryParameter("min_index", this.MinIndex.ToString());
			}

			if (this.MaxIndex >= 1) {
				request.AddQueryParameter("max_index", this.MaxIndex.ToString());
			}
		}
	}
}
