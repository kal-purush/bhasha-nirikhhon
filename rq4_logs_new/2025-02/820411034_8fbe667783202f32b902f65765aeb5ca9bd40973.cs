using static PCSSCommon.Clients.SearchDateServices.SearchDateClient.PcssCounsel;

namespace PCSSCommon.Clients.SearchDateServices;

[System.CodeDom.Compiler.GeneratedCode("NSwag", "14.2.0.0 (NJsonSchema v11.1.0.0 (Newtonsoft.Json v13.0.0.0))")]
public partial class SearchDateClient
{
#pragma warning disable 8618
    private string _baseUrl;
#pragma warning restore 8618

    private System.Net.Http.HttpClient _httpClient;
    private static System.Lazy<Newtonsoft.Json.JsonSerializerSettings> _settings = new System.Lazy<Newtonsoft.Json.JsonSerializerSettings>(CreateSerializerSettings, true);
    private Newtonsoft.Json.JsonSerializerSettings _instanceSettings;

#pragma warning disable CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.
    public SearchDateClient(System.Net.Http.HttpClient httpClient)
#pragma warning restore CS8618 // Non-nullable field must contain a non-null value when exiting constructor. Consider declaring as nullable.
    {
        _httpClient = httpClient;
        Initialize();
    }

    private static Newtonsoft.Json.JsonSerializerSettings CreateSerializerSettings()
    {
        var settings = new Newtonsoft.Json.JsonSerializerSettings();
        UpdateJsonSerializerSettings(settings);
        return settings;
    }

    public string BaseUrl
    {
        get { return _baseUrl; }
        set
        {
            _baseUrl = value;
            if (!string.IsNullOrEmpty(_baseUrl) && !_baseUrl.EndsWith("/"))
                _baseUrl += '/';
        }
    }

    protected Newtonsoft.Json.JsonSerializerSettings JsonSerializerSettings { get { return _instanceSettings ?? _settings.Value; } }

    static partial void UpdateJsonSerializerSettings(Newtonsoft.Json.JsonSerializerSettings settings);

    partial void Initialize();

    partial void PrepareRequest(System.Net.Http.HttpClient client, System.Net.Http.HttpRequestMessage request, string url);
    partial void PrepareRequest(System.Net.Http.HttpClient client, System.Net.Http.HttpRequestMessage request, System.Text.StringBuilder urlBuilder);
    partial void ProcessResponse(System.Net.Http.HttpClient client, System.Net.Http.HttpResponseMessage response);

    /// <returns>OK</returns>
    /// <exception cref="ApiException">A server side error occurred.</exception>
    public virtual System.Threading.Tasks.Task<System.Collections.Generic.ICollection<FindBestDateResult>> SearchDateAsync(FindBestDateParameters parameters)
    {
        return SearchDateAsync(parameters, System.Threading.CancellationToken.None);
    }

    /// <param name="cancellationToken">A cancellation token that can be used by other objects or threads to receive notice of cancellation.</param>
    /// <returns>OK</returns>
    /// <exception cref="ApiException">A server side error occurred.</exception>
    public virtual async System.Threading.Tasks.Task<System.Collections.Generic.ICollection<FindBestDateResult>> SearchDateAsync(FindBestDateParameters parameters, System.Threading.CancellationToken cancellationToken)
    {
        if (parameters == null)
            throw new System.ArgumentNullException("parameters");

        var client_ = _httpClient;
        var disposeClient_ = false;
        try
        {
            using (var request_ = new System.Net.Http.HttpRequestMessage())
            {
                var json_ = Newtonsoft.Json.JsonConvert.SerializeObject(parameters, JsonSerializerSettings);
                var dictionary_ = Newtonsoft.Json.JsonConvert.DeserializeObject<System.Collections.Generic.Dictionary<string, string>>(json_, JsonSerializerSettings);
                var content_ = new System.Net.Http.FormUrlEncodedContent(dictionary_);
                content_.Headers.ContentType = System.Net.Http.Headers.MediaTypeHeaderValue.Parse("application/json");
                request_.Content = content_;
                request_.Method = new System.Net.Http.HttpMethod("POST");
                request_.Headers.Accept.Add(System.Net.Http.Headers.MediaTypeWithQualityHeaderValue.Parse("application/json"));

                var urlBuilder_ = new System.Text.StringBuilder();
                if (!string.IsNullOrEmpty(_baseUrl)) urlBuilder_.Append(_baseUrl);
                // Operation Path: "api/searchDateAsync"
                urlBuilder_.Append("api/searchDateAsync");

                PrepareRequest(client_, request_, urlBuilder_);

                var url_ = urlBuilder_.ToString();
                request_.RequestUri = new System.Uri(url_, System.UriKind.RelativeOrAbsolute);

                PrepareRequest(client_, request_, url_);

                var response_ = await client_.SendAsync(request_, System.Net.Http.HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
                var disposeResponse_ = true;
                try
                {
                    var headers_ = new System.Collections.Generic.Dictionary<string, System.Collections.Generic.IEnumerable<string>>();
                    foreach (var item_ in response_.Headers)
                        headers_[item_.Key] = item_.Value;
                    if (response_.Content != null && response_.Content.Headers != null)
                    {
                        foreach (var item_ in response_.Content.Headers)
                            headers_[item_.Key] = item_.Value;
                    }

                    ProcessResponse(client_, response_);

                    var status_ = (int)response_.StatusCode;
                    if (status_ == 200)
                    {
                        var objectResponse_ = await ReadObjectResponseAsync<System.Collections.Generic.ICollection<FindBestDateResult>>(response_, headers_, cancellationToken).ConfigureAwait(false);
                        if (objectResponse_.Object == null)
                        {
                            throw new ApiException("Response was null which was not expected.", status_, objectResponse_.Text, headers_, null);
                        }
                        return objectResponse_.Object;
                    }
                    else
                    {
                        var responseData_ = response_.Content == null ? null : await response_.Content.ReadAsStringAsync().ConfigureAwait(false);
                        throw new ApiException("The HTTP status code of the response was not expected (" + status_ + ").", status_, responseData_, headers_, null);
                    }
                }
                finally
                {
                    if (disposeResponse_)
                        response_.Dispose();
                }
            }
        }
        finally
        {
            if (disposeClient_)
                client_.Dispose();
        }
    }

    /// <returns>OK</returns>
    /// <exception cref="ApiException">A server side error occurred.</exception>
    public virtual System.Threading.Tasks.Task<ActivityAppearanceResultsCollection> GetAppearancesCollectionAsync(int locationId, string date, string showAslDataYn)
    {
        return GetAppearancesCollectionAsync(locationId, date, showAslDataYn, System.Threading.CancellationToken.None);
    }

    /// <param name="cancellationToken">A cancellation token that can be used by other objects or threads to receive notice of cancellation.</param>
    /// <returns>OK</returns>
    /// <exception cref="ApiException">A server side error occurred.</exception>
    public virtual async System.Threading.Tasks.Task<ActivityAppearanceResultsCollection> GetAppearancesCollectionAsync(int locationId, string date, string showAslDataYn, System.Threading.CancellationToken cancellationToken)
    {
        if (locationId == null)
            throw new System.ArgumentNullException("locationId");

        if (date == null)
            throw new System.ArgumentNullException("date");

        var client_ = _httpClient;
        var disposeClient_ = false;
        try
        {
            using (var request_ = new System.Net.Http.HttpRequestMessage())
            {
                request_.Method = new System.Net.Http.HttpMethod("GET");
                request_.Headers.Accept.Add(System.Net.Http.Headers.MediaTypeWithQualityHeaderValue.Parse("application/json"));

                var urlBuilder_ = new System.Text.StringBuilder();
                if (!string.IsNullOrEmpty(_baseUrl)) urlBuilder_.Append(_baseUrl);
                // Operation Path: "api/appearances/{locationId}"
                urlBuilder_.Append("api/appearances/");
                urlBuilder_.Append(System.Uri.EscapeDataString(ConvertToString(locationId, System.Globalization.CultureInfo.InvariantCulture)));
                urlBuilder_.Append('?');
                urlBuilder_.Append(System.Uri.EscapeDataString("date")).Append('=').Append(System.Uri.EscapeDataString(ConvertToString(date, System.Globalization.CultureInfo.InvariantCulture))).Append('&');
                if (showAslDataYn != null)
                {
                    urlBuilder_.Append(System.Uri.EscapeDataString("showAslDataYn")).Append('=').Append(System.Uri.EscapeDataString(ConvertToString(showAslDataYn, System.Globalization.CultureInfo.InvariantCulture))).Append('&');
                }
                urlBuilder_.Length--;

                PrepareRequest(client_, request_, urlBuilder_);

                var url_ = urlBuilder_.ToString();
                request_.RequestUri = new System.Uri(url_, System.UriKind.RelativeOrAbsolute);

                PrepareRequest(client_, request_, url_);

                var response_ = await client_.SendAsync(request_, System.Net.Http.HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
                var disposeResponse_ = true;
                try
                {
                    var headers_ = new System.Collections.Generic.Dictionary<string, System.Collections.Generic.IEnumerable<string>>();
                    foreach (var item_ in response_.Headers)
                        headers_[item_.Key] = item_.Value;
                    if (response_.Content != null && response_.Content.Headers != null)
                    {
                        foreach (var item_ in response_.Content.Headers)
                            headers_[item_.Key] = item_.Value;
                    }

                    ProcessResponse(client_, response_);

                    var status_ = (int)response_.StatusCode;
                    if (status_ == 200)
                    {
                        var objectResponse_ = await ReadObjectResponseAsync<ActivityAppearanceResultsCollection>(response_, headers_, cancellationToken).ConfigureAwait(false);
                        if (objectResponse_.Object == null)
                        {
                            throw new ApiException("Response was null which was not expected.", status_, objectResponse_.Text, headers_, null);
                        }
                        return objectResponse_.Object;
                    }
                    else
                    {
                        var responseData_ = response_.Content == null ? null : await response_.Content.ReadAsStringAsync().ConfigureAwait(false);
                        throw new ApiException("The HTTP status code of the response was not expected (" + status_ + ").", status_, responseData_, headers_, null);
                    }
                }
                finally
                {
                    if (disposeResponse_)
                        response_.Dispose();
                }
            }
        }
        finally
        {
            if (disposeClient_)
                client_.Dispose();
        }
    }

    /// <returns>OK</returns>
    /// <exception cref="ApiException">A server side error occurred.</exception>
    public virtual System.Threading.Tasks.Task<System.Collections.Generic.ICollection<ActivityAppearanceDetail>> GetStoodDownAppearancesAsync(int locationId, string date, string showAslDataYn)
    {
        return GetStoodDownAppearancesAsync(locationId, date, showAslDataYn, System.Threading.CancellationToken.None);
    }

    /// <param name="cancellationToken">A cancellation token that can be used by other objects or threads to receive notice of cancellation.</param>
    /// <returns>OK</returns>
    /// <exception cref="ApiException">A server side error occurred.</exception>
    public virtual async System.Threading.Tasks.Task<System.Collections.Generic.ICollection<ActivityAppearanceDetail>> GetStoodDownAppearancesAsync(int locationId, string date, string showAslDataYn, System.Threading.CancellationToken cancellationToken)
    {
        if (locationId == null)
            throw new System.ArgumentNullException("locationId");

        if (date == null)
            throw new System.ArgumentNullException("date");

        var client_ = _httpClient;
        var disposeClient_ = false;
        try
        {
            using (var request_ = new System.Net.Http.HttpRequestMessage())
            {
                request_.Method = new System.Net.Http.HttpMethod("GET");
                request_.Headers.Accept.Add(System.Net.Http.Headers.MediaTypeWithQualityHeaderValue.Parse("application/json"));

                var urlBuilder_ = new System.Text.StringBuilder();
                if (!string.IsNullOrEmpty(_baseUrl)) urlBuilder_.Append(_baseUrl);
                // Operation Path: "api/appearances/stoodDown"
                urlBuilder_.Append("api/appearances/stoodDown");
                urlBuilder_.Append('?');
                urlBuilder_.Append(System.Uri.EscapeDataString("locationId")).Append('=').Append(System.Uri.EscapeDataString(ConvertToString(locationId, System.Globalization.CultureInfo.InvariantCulture))).Append('&');
                urlBuilder_.Append(System.Uri.EscapeDataString("date")).Append('=').Append(System.Uri.EscapeDataString(ConvertToString(date, System.Globalization.CultureInfo.InvariantCulture))).Append('&');
                if (showAslDataYn != null)
                {
                    urlBuilder_.Append(System.Uri.EscapeDataString("showAslDataYn")).Append('=').Append(System.Uri.EscapeDataString(ConvertToString(showAslDataYn, System.Globalization.CultureInfo.InvariantCulture))).Append('&');
                }
                urlBuilder_.Length--;

                PrepareRequest(client_, request_, urlBuilder_);

                var url_ = urlBuilder_.ToString();
                request_.RequestUri = new System.Uri(url_, System.UriKind.RelativeOrAbsolute);

                PrepareRequest(client_, request_, url_);

                var response_ = await client_.SendAsync(request_, System.Net.Http.HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
                var disposeResponse_ = true;
                try
                {
                    var headers_ = new System.Collections.Generic.Dictionary<string, System.Collections.Generic.IEnumerable<string>>();
                    foreach (var item_ in response_.Headers)
                        headers_[item_.Key] = item_.Value;
                    if (response_.Content != null && response_.Content.Headers != null)
                    {
                        foreach (var item_ in response_.Content.Headers)
                            headers_[item_.Key] = item_.Value;
                    }

                    ProcessResponse(client_, response_);

                    var status_ = (int)response_.StatusCode;
                    if (status_ == 200)
                    {
                        var objectResponse_ = await ReadObjectResponseAsync<System.Collections.Generic.ICollection<ActivityAppearanceDetail>>(response_, headers_, cancellationToken).ConfigureAwait(false);
                        if (objectResponse_.Object == null)
                        {
                            throw new ApiException("Response was null which was not expected.", status_, objectResponse_.Text, headers_, null);
                        }
                        return objectResponse_.Object;
                    }
                    else
                    {
                        var responseData_ = response_.Content == null ? null : await response_.Content.ReadAsStringAsync().ConfigureAwait(false);
                        throw new ApiException("The HTTP status code of the response was not expected (" + status_ + ").", status_, responseData_, headers_, null);
                    }
                }
                finally
                {
                    if (disposeResponse_)
                        response_.Dispose();
                }
            }
        }
        finally
        {
            if (disposeClient_)
                client_.Dispose();
        }
    }

    /// <returns>OK</returns>
    /// <exception cref="ApiException">A server side error occurred.</exception>
    public virtual System.Threading.Tasks.Task<System.Collections.Generic.ICollection<ActivityAppearanceDetail>> NoOpAsync(int locationId, string date, string showAslDataYn)
    {
        return NoOpAsync(locationId, date, showAslDataYn, System.Threading.CancellationToken.None);
    }

    /// <param name="cancellationToken">A cancellation token that can be used by other objects or threads to receive notice of cancellation.</param>
    /// <returns>OK</returns>
    /// <exception cref="ApiException">A server side error occurred.</exception>
    public virtual async System.Threading.Tasks.Task<System.Collections.Generic.ICollection<ActivityAppearanceDetail>> NoOpAsync(int locationId, string date, string showAslDataYn, System.Threading.CancellationToken cancellationToken)
    {
        if (locationId == null)
            throw new System.ArgumentNullException("locationId");

        if (date == null)
            throw new System.ArgumentNullException("date");

        var client_ = _httpClient;
        var disposeClient_ = false;
        try
        {
            using (var request_ = new System.Net.Http.HttpRequestMessage())
            {
                request_.Method = new System.Net.Http.HttpMethod("GET");
                request_.Headers.Accept.Add(System.Net.Http.Headers.MediaTypeWithQualityHeaderValue.Parse("application/json"));

                var urlBuilder_ = new System.Text.StringBuilder();
                if (!string.IsNullOrEmpty(_baseUrl)) urlBuilder_.Append(_baseUrl);
                // Operation Path: "api/appearances/trialtracker"
                urlBuilder_.Append("api/appearances/trialtracker");
                urlBuilder_.Append('?');
                urlBuilder_.Append(System.Uri.EscapeDataString("locationId")).Append('=').Append(System.Uri.EscapeDataString(ConvertToString(locationId, System.Globalization.CultureInfo.InvariantCulture))).Append('&');
                urlBuilder_.Append(System.Uri.EscapeDataString("date")).Append('=').Append(System.Uri.EscapeDataString(ConvertToString(date, System.Globalization.CultureInfo.InvariantCulture))).Append('&');
                if (showAslDataYn != null)
                {
                    urlBuilder_.Append(System.Uri.EscapeDataString("showAslDataYn")).Append('=').Append(System.Uri.EscapeDataString(ConvertToString(showAslDataYn, System.Globalization.CultureInfo.InvariantCulture))).Append('&');
                }
                urlBuilder_.Length--;

                PrepareRequest(client_, request_, urlBuilder_);

                var url_ = urlBuilder_.ToString();
                request_.RequestUri = new System.Uri(url_, System.UriKind.RelativeOrAbsolute);

                PrepareRequest(client_, request_, url_);

                var response_ = await client_.SendAsync(request_, System.Net.Http.HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
                var disposeResponse_ = true;
                try
                {
                    var headers_ = new System.Collections.Generic.Dictionary<string, System.Collections.Generic.IEnumerable<string>>();
                    foreach (var item_ in response_.Headers)
                        headers_[item_.Key] = item_.Value;
                    if (response_.Content != null && response_.Content.Headers != null)
                    {
                        foreach (var item_ in response_.Content.Headers)
                            headers_[item_.Key] = item_.Value;
                    }

                    ProcessResponse(client_, response_);

                    var status_ = (int)response_.StatusCode;
                    if (status_ == 200)
                    {
                        var objectResponse_ = await ReadObjectResponseAsync<System.Collections.Generic.ICollection<ActivityAppearanceDetail>>(response_, headers_, cancellationToken).ConfigureAwait(false);
                        if (objectResponse_.Object == null)
                        {
                            throw new ApiException("Response was null which was not expected.", status_, objectResponse_.Text, headers_, null);
                        }
                        return objectResponse_.Object;
                    }
                    else
                    {
                        var responseData_ = response_.Content == null ? null : await response_.Content.ReadAsStringAsync().ConfigureAwait(false);
                        throw new ApiException("The HTTP status code of the response was not expected (" + status_ + ").", status_, responseData_, headers_, null);
                    }
                }
                finally
                {
                    if (disposeResponse_)
                        response_.Dispose();
                }
            }
        }
        finally
        {
            if (disposeClient_)
                client_.Dispose();
        }
    }

    /// <returns>OK</returns>
    /// <exception cref="ApiException">A server side error occurred.</exception>
    public virtual System.Threading.Tasks.Task<ActivityAppearanceResultsCollection> GetCourtListAppearancesAsync(int locationId, string date, int judiciaryPersonId, string courtRoomCd, bool? jumpToAssignment)
    {
        return GetCourtListAppearancesAsync(locationId, date, judiciaryPersonId, courtRoomCd, jumpToAssignment, System.Threading.CancellationToken.None);
    }

    /// <param name="cancellationToken">A cancellation token that can be used by other objects or threads to receive notice of cancellation.</param>
    /// <returns>OK</returns>
    /// <exception cref="ApiException">A server side error occurred.</exception>
    public virtual async System.Threading.Tasks.Task<ActivityAppearanceResultsCollection> GetCourtListAppearancesAsync(int locationId, string date, int judiciaryPersonId, string courtRoomCd, bool? jumpToAssignment, System.Threading.CancellationToken cancellationToken)
    {
        if (locationId == null)
            throw new System.ArgumentNullException("locationId");

        if (date == null)
            throw new System.ArgumentNullException("date");

        if (judiciaryPersonId == null)
            throw new System.ArgumentNullException("judiciaryPersonId");

        var client_ = _httpClient;
        var disposeClient_ = false;
        try
        {
            using (var request_ = new System.Net.Http.HttpRequestMessage())
            {
                request_.Method = new System.Net.Http.HttpMethod("GET");
                request_.Headers.Accept.Add(System.Net.Http.Headers.MediaTypeWithQualityHeaderValue.Parse("application/json"));

                var urlBuilder_ = new System.Text.StringBuilder();
                if (!string.IsNullOrEmpty(_baseUrl)) urlBuilder_.Append(_baseUrl);
                // Operation Path: "api/appearances/courtlist"
                urlBuilder_.Append("api/appearances/courtlist");
                urlBuilder_.Append('?');
                urlBuilder_.Append(System.Uri.EscapeDataString("locationId")).Append('=').Append(System.Uri.EscapeDataString(ConvertToString(locationId, System.Globalization.CultureInfo.InvariantCulture))).Append('&');
                urlBuilder_.Append(System.Uri.EscapeDataString("date")).Append('=').Append(System.Uri.EscapeDataString(ConvertToString(date, System.Globalization.CultureInfo.InvariantCulture))).Append('&');
                urlBuilder_.Append(System.Uri.EscapeDataString("judiciaryPersonId")).Append('=').Append(System.Uri.EscapeDataString(ConvertToString(judiciaryPersonId, System.Globalization.CultureInfo.InvariantCulture))).Append('&');
                if (courtRoomCd != null)
                {
                    urlBuilder_.Append(System.Uri.EscapeDataString("courtRoomCd")).Append('=').Append(System.Uri.EscapeDataString(ConvertToString(courtRoomCd, System.Globalization.CultureInfo.InvariantCulture))).Append('&');
                }
                if (jumpToAssignment != null)
                {
                    urlBuilder_.Append(System.Uri.EscapeDataString("jumpToAssignment")).Append('=').Append(System.Uri.EscapeDataString(ConvertToString(jumpToAssignment, System.Globalization.CultureInfo.InvariantCulture))).Append('&');
                }
                urlBuilder_.Length--;

                PrepareRequest(client_, request_, urlBuilder_);

                var url_ = urlBuilder_.ToString();
                request_.RequestUri = new System.Uri(url_, System.UriKind.RelativeOrAbsolute);

                PrepareRequest(client_, request_, url_);

                var response_ = await client_.SendAsync(request_, System.Net.Http.HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
                var disposeResponse_ = true;
                try
                {
                    var headers_ = new System.Collections.Generic.Dictionary<string, System.Collections.Generic.IEnumerable<string>>();
                    foreach (var item_ in response_.Headers)
                        headers_[item_.Key] = item_.Value;
                    if (response_.Content != null && response_.Content.Headers != null)
                    {
                        foreach (var item_ in response_.Content.Headers)
                            headers_[item_.Key] = item_.Value;
                    }

                    ProcessResponse(client_, response_);

                    var status_ = (int)response_.StatusCode;
                    if (status_ == 200)
                    {
                        var objectResponse_ = await ReadObjectResponseAsync<ActivityAppearanceResultsCollection>(response_, headers_, cancellationToken).ConfigureAwait(false);
                        if (objectResponse_.Object == null)
                        {
                            throw new ApiException("Response was null which was not expected.", status_, objectResponse_.Text, headers_, null);
                        }
                        return objectResponse_.Object;
                    }
                    else
                    {
                        var responseData_ = response_.Content == null ? null : await response_.Content.ReadAsStringAsync().ConfigureAwait(false);
                        throw new ApiException("The HTTP status code of the response was not expected (" + status_ + ").", status_, responseData_, headers_, null);
                    }
                }
                finally
                {
                    if (disposeResponse_)
                        response_.Dispose();
                }
            }
        }
        finally
        {
            if (disposeClient_)
                client_.Dispose();
        }
    }

    public bool ReadResponseAsString { get; set; }

    protected virtual async System.Threading.Tasks.Task<ObjectResponseResult<T>> ReadObjectResponseAsync<T>(System.Net.Http.HttpResponseMessage response, System.Collections.Generic.IReadOnlyDictionary<string, System.Collections.Generic.IEnumerable<string>> headers, System.Threading.CancellationToken cancellationToken)
    {
        if (response == null || response.Content == null)
        {
            return new ObjectResponseResult<T>(default(T), string.Empty);
        }

        if (ReadResponseAsString)
        {
            var responseText = await response.Content.ReadAsStringAsync().ConfigureAwait(false);
            try
            {
                var typedBody = Newtonsoft.Json.JsonConvert.DeserializeObject<T>(responseText, JsonSerializerSettings);
                return new ObjectResponseResult<T>(typedBody, responseText);
            }
            catch (Newtonsoft.Json.JsonException exception)
            {
                var message = "Could not deserialize the response body string as " + typeof(T).FullName + ".";
                throw new ApiException(message, (int)response.StatusCode, responseText, headers, exception);
            }
        }
        else
        {
            try
            {
                using (var responseStream = await response.Content.ReadAsStreamAsync().ConfigureAwait(false))
                using (var streamReader = new System.IO.StreamReader(responseStream))
                using (var jsonTextReader = new Newtonsoft.Json.JsonTextReader(streamReader))
                {
                    var serializer = Newtonsoft.Json.JsonSerializer.Create(JsonSerializerSettings);
                    var typedBody = serializer.Deserialize<T>(jsonTextReader);
                    return new ObjectResponseResult<T>(typedBody, string.Empty);
                }
            }
            catch (Newtonsoft.Json.JsonException exception)
            {
                var message = "Could not deserialize the response body stream as " + typeof(T).FullName + ".";
                throw new ApiException(message, (int)response.StatusCode, string.Empty, headers, exception);
            }
        }
    }
    protected struct ObjectResponseResult<T>
    {
        public ObjectResponseResult(T responseObject, string responseText)
        {
            this.Object = responseObject;
            this.Text = responseText;
        }

        public T Object { get; }

        public string Text { get; }
    }

    private string ConvertToString(object value, System.Globalization.CultureInfo cultureInfo)
    {
        if (value == null)
        {
            return "";
        }

        if (value is System.Enum)
        {
            var name = System.Enum.GetName(value.GetType(), value);
            if (name != null)
            {
                var field = System.Reflection.IntrospectionExtensions.GetTypeInfo(value.GetType()).GetDeclaredField(name);
                if (field != null)
                {
                    var attribute = System.Reflection.CustomAttributeExtensions.GetCustomAttribute(field, typeof(System.Runtime.Serialization.EnumMemberAttribute))
                        as System.Runtime.Serialization.EnumMemberAttribute;
                    if (attribute != null)
                    {
                        return attribute.Value != null ? attribute.Value : name;
                    }
                }

                var converted = System.Convert.ToString(System.Convert.ChangeType(value, System.Enum.GetUnderlyingType(value.GetType()), cultureInfo));
                return converted == null ? string.Empty : converted;
            }
        }
        else if (value is bool)
        {
            return System.Convert.ToString((bool)value, cultureInfo).ToLowerInvariant();
        }
        else if (value is byte[])
        {
            return System.Convert.ToBase64String((byte[])value);
        }
        else if (value is string[])
        {
            return string.Join(",", (string[])value);
        }
        else if (value.GetType().IsArray)
        {
            var valueArray = (System.Array)value;
            var valueTextArray = new string[valueArray.Length];
            for (var i = 0; i < valueArray.Length; i++)
            {
                valueTextArray[i] = ConvertToString(valueArray.GetValue(i), cultureInfo);
            }
            return string.Join(",", valueTextArray);
        }

        var result = System.Convert.ToString(value, cultureInfo);
        return result == null ? "" : result;
    }

    /// <returns>OK</returns>
    /// <exception cref="ApiException">A server side error occurred.</exception>
    public virtual System.Threading.Tasks.Task<System.Collections.Generic.ICollection<Appearance2>> GetAppearancesAsync(string justinApprIds, string slotIds)
    {
        return GetAppearancesAsync(justinApprIds, slotIds, System.Threading.CancellationToken.None);
    }

    /// <param name="cancellationToken">A cancellation token that can be used by other objects or threads to receive notice of cancellation.</param>
    /// <returns>OK</returns>
    /// <exception cref="ApiException">A server side error occurred.</exception>
    public virtual async System.Threading.Tasks.Task<System.Collections.Generic.ICollection<Appearance2>> GetAppearancesAsync(string justinApprIds, string slotIds, System.Threading.CancellationToken cancellationToken)
    {
        var client_ = _httpClient;
        var disposeClient_ = false;
        try
        {
            using (var request_ = new System.Net.Http.HttpRequestMessage())
            {
                request_.Method = new System.Net.Http.HttpMethod("GET");
                request_.Headers.Accept.Add(System.Net.Http.Headers.MediaTypeWithQualityHeaderValue.Parse("application/json"));

                var urlBuilder_ = new System.Text.StringBuilder();
                if (!string.IsNullOrEmpty(_baseUrl)) urlBuilder_.Append(_baseUrl);
                // Operation Path: "api/appearances"
                urlBuilder_.Append("api/appearances");
                urlBuilder_.Append('?');
                if (justinApprIds != null)
                {
                    urlBuilder_.Append(System.Uri.EscapeDataString("justinApprIds")).Append('=').Append(System.Uri.EscapeDataString(ConvertToString(justinApprIds, System.Globalization.CultureInfo.InvariantCulture))).Append('&');
                }
                if (slotIds != null)
                {
                    urlBuilder_.Append(System.Uri.EscapeDataString("slotIds")).Append('=').Append(System.Uri.EscapeDataString(ConvertToString(slotIds, System.Globalization.CultureInfo.InvariantCulture))).Append('&');
                }
                urlBuilder_.Length--;

                PrepareRequest(client_, request_, urlBuilder_);

                var url_ = urlBuilder_.ToString();
                request_.RequestUri = new System.Uri(url_, System.UriKind.RelativeOrAbsolute);

                PrepareRequest(client_, request_, url_);

                var response_ = await client_.SendAsync(request_, System.Net.Http.HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
                var disposeResponse_ = true;
                try
                {
                    var headers_ = new System.Collections.Generic.Dictionary<string, System.Collections.Generic.IEnumerable<string>>();
                    foreach (var item_ in response_.Headers)
                        headers_[item_.Key] = item_.Value;
                    if (response_.Content != null && response_.Content.Headers != null)
                    {
                        foreach (var item_ in response_.Content.Headers)
                            headers_[item_.Key] = item_.Value;
                    }

                    ProcessResponse(client_, response_);

                    var status_ = (int)response_.StatusCode;
                    if (status_ == 200)
                    {
                        var objectResponse_ = await ReadObjectResponseAsync<System.Collections.Generic.ICollection<Appearance2>>(response_, headers_, cancellationToken).ConfigureAwait(false);
                        if (objectResponse_.Object == null)
                        {
                            throw new ApiException("Response was null which was not expected.", status_, objectResponse_.Text, headers_, null);
                        }
                        return objectResponse_.Object;
                    }
                    else
                    {
                        var responseData_ = response_.Content == null ? null : await response_.Content.ReadAsStringAsync().ConfigureAwait(false);
                        throw new ApiException("The HTTP status code of the response was not expected (" + status_ + ").", status_, responseData_, headers_, null);
                    }
                }
                finally
                {
                    if (disposeResponse_)
                        response_.Dispose();
                }
            }
        }
        finally
        {
            if (disposeClient_)
                client_.Dispose();
        }
    }

    [System.CodeDom.Compiler.GeneratedCode("NJsonSchema", "14.2.0.0 (NJsonSchema v11.1.0.0 (Newtonsoft.Json v13.0.0.0))")]
    public partial class Appearance2
    {
        [Newtonsoft.Json.JsonProperty("pcssAppearanceId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public int? PcssAppearanceId { get; set; }

        [Newtonsoft.Json.JsonProperty("locationId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public int? LocationId { get; set; }

        [Newtonsoft.Json.JsonProperty("activityClassCode", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string ActivityClassCode { get; set; }

        [Newtonsoft.Json.JsonProperty("courtRoomCode", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string CourtRoomCode { get; set; }

        [Newtonsoft.Json.JsonProperty("reasonCode", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string ReasonCode { get; set; }

        [Newtonsoft.Json.JsonProperty("time", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public System.DateTimeOffset? Time { get; set; }

        [Newtonsoft.Json.JsonProperty("date", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public System.DateTimeOffset? Date { get; set; }

        [Newtonsoft.Json.JsonProperty("cancelled", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public bool? Cancelled { get; set; }

        [Newtonsoft.Json.JsonProperty("confirmed", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public bool? Confirmed { get; set; }

        [Newtonsoft.Json.JsonProperty("fileNumberText", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string FileNumberText { get; set; }

        [Newtonsoft.Json.JsonProperty("profSequenceNumber", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string ProfSequenceNumber { get; set; }

        [Newtonsoft.Json.JsonProperty("participantId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string ParticipantId { get; set; }

        [Newtonsoft.Json.JsonProperty("justinNumber", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string JustinNumber { get; set; }

        [Newtonsoft.Json.JsonProperty("courtActivitySlotId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public int? CourtActivitySlotId { get; set; }

        [Newtonsoft.Json.JsonProperty("estimatedQty", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public double? EstimatedQty { get; set; }

        [Newtonsoft.Json.JsonProperty("estimatedUnitCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string EstimatedUnitCd { get; set; }

        [Newtonsoft.Json.JsonProperty("active", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public bool? Active { get; set; }

        [Newtonsoft.Json.JsonProperty("physicalFileId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string PhysicalFileId { get; set; }

        [Newtonsoft.Json.JsonProperty("ceisAppearanceId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string CeisAppearanceId { get; set; }

        [Newtonsoft.Json.JsonProperty("justinAppearanceId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string JustinAppearanceId { get; set; }

        [Newtonsoft.Json.JsonProperty("courtLevelCode", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string CourtLevelCode { get; set; }

    }

    [System.CodeDom.Compiler.GeneratedCode("NJsonSchema", "14.2.0.0 (NJsonSchema v11.1.0.0 (Newtonsoft.Json v13.0.0.0))")]
    public partial class PersonnelCommitment
    {
        [Newtonsoft.Json.JsonProperty("commitmentCount", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string CommitmentCount { get; set; }

        [Newtonsoft.Json.JsonProperty("commitmentTypeDsc", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string CommitmentTypeDsc { get; set; }

        [Newtonsoft.Json.JsonProperty("activityTypeCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string ActivityTypeCd { get; set; }

        [Newtonsoft.Json.JsonProperty("activityTypeDsc", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string ActivityTypeDsc { get; set; }

        [Newtonsoft.Json.JsonProperty("createdDt", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string CreatedDt { get; set; }

        [Newtonsoft.Json.JsonProperty("courtAgencyId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string CourtAgencyId { get; set; }

        [Newtonsoft.Json.JsonProperty("locationId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public int? LocationId { get; set; }

        [Newtonsoft.Json.JsonProperty("locationNm", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string LocationNm { get; set; }

        [Newtonsoft.Json.JsonProperty("regionNm", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string RegionNm { get; set; }

        [Newtonsoft.Json.JsonProperty("courtRoomCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string CourtRoomCd { get; set; }

        [Newtonsoft.Json.JsonProperty("commitmentDt", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string CommitmentDt { get; set; }

        [Newtonsoft.Json.JsonProperty("commitmentTm", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string CommitmentTm { get; set; }

        [Newtonsoft.Json.JsonProperty("durationHour", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string DurationHour { get; set; }

        [Newtonsoft.Json.JsonProperty("durationMin", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string DurationMin { get; set; }

        [Newtonsoft.Json.JsonProperty("courtFileNo", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string CourtFileNo { get; set; }

        [Newtonsoft.Json.JsonProperty("commitmentTxt", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string CommitmentTxt { get; set; }

    }


    [System.CodeDom.Compiler.GeneratedCode("NJsonSchema", "14.2.0.0 (NJsonSchema v11.1.0.0 (Newtonsoft.Json v13.0.0.0))")]
    public partial class PersonnelAvailability
    {
        [Newtonsoft.Json.JsonProperty("partId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string PartId { get; set; }

        [Newtonsoft.Json.JsonProperty("fullNm", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string FullNm { get; set; }

        [Newtonsoft.Json.JsonProperty("availabilityCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string AvailabilityCd { get; set; }

        [Newtonsoft.Json.JsonProperty("availabilityDsc", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string AvailabilityDsc { get; set; }

        [Newtonsoft.Json.JsonProperty("availabilityWeightFactorCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public int? AvailabilityWeightFactorCd { get; set; }

        [Newtonsoft.Json.JsonProperty("date", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public System.DateTimeOffset? Date { get; set; }

        [Newtonsoft.Json.JsonProperty("dateStr", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string DateStr { get; set; }

        [Newtonsoft.Json.JsonProperty("personTypeCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string PersonTypeCd { get; set; }

        [Newtonsoft.Json.JsonProperty("commitments", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public System.Collections.Generic.ICollection<PersonnelCommitment> Commitments { get; set; }

        [Newtonsoft.Json.JsonProperty("pinCodeTxt", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string PinCodeTxt { get; set; }

        [Newtonsoft.Json.JsonProperty("agencyDsc", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string AgencyDsc { get; set; }

        [Newtonsoft.Json.JsonProperty("agencyCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string AgencyCd { get; set; }

        [Newtonsoft.Json.JsonProperty("ccssAvailabilityCode", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string CcssAvailabilityCode { get; set; }

        [Newtonsoft.Json.JsonProperty("ccssAvailabilityNoteToJCM", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string CcssAvailabilityNoteToJCM { get; set; }

    }

    [System.CodeDom.Compiler.GeneratedCode("NJsonSchema", "14.2.0.0 (NJsonSchema v11.1.0.0 (Newtonsoft.Json v13.0.0.0))")]
    public partial class ActivityClassUsage
    {
        [Newtonsoft.Json.JsonProperty("activityClassCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string ActivityClassCd { get; set; }

        [Newtonsoft.Json.JsonProperty("numberOfCases", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public int? NumberOfCases { get; set; }

        [Newtonsoft.Json.JsonProperty("numberOfHours", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public double? NumberOfHours { get; set; }

        [Newtonsoft.Json.JsonProperty("activityClassDsc", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string ActivityClassDsc { get; set; }

        [Newtonsoft.Json.JsonProperty("capacityScore", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public double? CapacityScore { get; set; }

        [Newtonsoft.Json.JsonProperty("totalQuantity", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public double? TotalQuantity { get; set; }

        [Newtonsoft.Json.JsonProperty("usedQuantity", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public double? UsedQuantity { get; set; }

    }

    [System.CodeDom.Compiler.GeneratedCode("NJsonSchema", "14.2.0.0 (NJsonSchema v11.1.0.0 (Newtonsoft.Json v13.0.0.0))")]
    public partial class OfferedDate2
    {
        [Newtonsoft.Json.JsonProperty("declineRoleCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string DeclineRoleCd { get; set; }

        [Newtonsoft.Json.JsonProperty("offeredDt", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public System.DateTimeOffset? OfferedDt { get; set; }

        [Newtonsoft.Json.JsonProperty("declineReasonTxt", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string DeclineReasonTxt { get; set; }

        [Newtonsoft.Json.JsonProperty("justinNo", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public int? JustinNo { get; set; }

        [Newtonsoft.Json.JsonProperty("physicalFileId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public double? PhysicalFileId { get; set; }

        [Newtonsoft.Json.JsonProperty("offeredDtStr", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string OfferedDtStr { get; set; }

    }

    [System.CodeDom.Compiler.GeneratedCode("NJsonSchema", "14.2.0.0 (NJsonSchema v11.1.0.0 (Newtonsoft.Json v13.0.0.0))")]
    public partial class CounselAvailabilityDetail
    {
        [Newtonsoft.Json.JsonProperty("appearanceTm", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string AppearanceTm { get; set; }

        [Newtonsoft.Json.JsonProperty("locationId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public int? LocationId { get; set; }

        [Newtonsoft.Json.JsonProperty("locationNm", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string LocationNm { get; set; }

        [Newtonsoft.Json.JsonProperty("courtRoomCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string CourtRoomCd { get; set; }

        [Newtonsoft.Json.JsonProperty("estimatedQty", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public double? EstimatedQty { get; set; }

        [Newtonsoft.Json.JsonProperty("estimatedUnitCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string EstimatedUnitCd { get; set; }

        [Newtonsoft.Json.JsonProperty("courtFileNumber", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string CourtFileNumber { get; set; }

        [Newtonsoft.Json.JsonProperty("pcssAppearanceId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public int? PcssAppearanceId { get; set; }

        [Newtonsoft.Json.JsonProperty("justinNo", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string JustinNo { get; set; }

        [Newtonsoft.Json.JsonProperty("physicalFileId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string PhysicalFileId { get; set; }

    }

    [System.CodeDom.Compiler.GeneratedCode("NJsonSchema", "14.2.0.0 (NJsonSchema v11.1.0.0 (Newtonsoft.Json v13.0.0.0))")]
    public partial class CounselAvailability
    {
        [Newtonsoft.Json.JsonProperty("counselId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public int? CounselId { get; set; }

        [Newtonsoft.Json.JsonProperty("lawSocietyId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public int? LawSocietyId { get; set; }

        [Newtonsoft.Json.JsonProperty("lastNm", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string LastNm { get; set; }

        [Newtonsoft.Json.JsonProperty("givenNm", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string GivenNm { get; set; }

        [Newtonsoft.Json.JsonProperty("prefNm", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string PrefNm { get; set; }

        [Newtonsoft.Json.JsonProperty("fullNm", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string FullNm { get; set; }

        [Newtonsoft.Json.JsonProperty("orgNm", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string OrgNm { get; set; }

        [Newtonsoft.Json.JsonProperty("date", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public System.DateTimeOffset? Date { get; set; }

        [Newtonsoft.Json.JsonProperty("dateStr", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string DateStr { get; set; }

        [Newtonsoft.Json.JsonProperty("details", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public System.Collections.Generic.ICollection<CounselAvailabilityDetail> Details { get; set; }

        [Newtonsoft.Json.JsonProperty("availabilityCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string AvailabilityCd { get; set; }

    }

    [System.CodeDom.Compiler.GeneratedCode("NJsonSchema", "14.2.0.0 (NJsonSchema v11.1.0.0 (Newtonsoft.Json v13.0.0.0))")]
    public partial class JudgeAssignmentDetail
    {
        [Newtonsoft.Json.JsonProperty("locationId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public int? LocationId { get; set; }

        [Newtonsoft.Json.JsonProperty("locationNm", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string LocationNm { get; set; }

        [Newtonsoft.Json.JsonProperty("judgeActivityCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string JudgeActivityCd { get; set; }

        [Newtonsoft.Json.JsonProperty("judgeActivityDsc", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string JudgeActivityDsc { get; set; }

        [Newtonsoft.Json.JsonProperty("courtActivityCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string CourtActivityCd { get; set; }

        [Newtonsoft.Json.JsonProperty("courtActivityDsc", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string CourtActivityDsc { get; set; }

        [Newtonsoft.Json.JsonProperty("courtLocationId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public int? CourtLocationId { get; set; }

        [Newtonsoft.Json.JsonProperty("courtLocationNm", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string CourtLocationNm { get; set; }

        [Newtonsoft.Json.JsonProperty("courtRoomCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string CourtRoomCd { get; set; }

        [Newtonsoft.Json.JsonProperty("commentTxt", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string CommentTxt { get; set; }

        [Newtonsoft.Json.JsonProperty("videoYn", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string VideoYn { get; set; }

        [Newtonsoft.Json.JsonProperty("courtSittingCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string CourtSittingCd { get; set; }

    }

    [System.CodeDom.Compiler.GeneratedCode("NJsonSchema", "14.2.0.0 (NJsonSchema v11.1.0.0 (Newtonsoft.Json v13.0.0.0))")]
    public partial class AdjudicatorRestriction
    {
        [Newtonsoft.Json.JsonProperty("pk", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string Pk { get; set; }

        [Newtonsoft.Json.JsonProperty("fileName", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string FileName { get; set; }

        [Newtonsoft.Json.JsonProperty("judgeName", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string JudgeName { get; set; }

        [Newtonsoft.Json.JsonProperty("appearanceReasonCode", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string AppearanceReasonCode { get; set; }

        [Newtonsoft.Json.JsonProperty("appearanceReasonDescription", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string AppearanceReasonDescription { get; set; }

        [Newtonsoft.Json.JsonProperty("restrictionCode", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string RestrictionCode { get; set; }

        [Newtonsoft.Json.JsonProperty("hasIssue", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public bool? HasIssue { get; set; }

        [Newtonsoft.Json.JsonProperty("activityCode", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string ActivityCode { get; set; }

        [Newtonsoft.Json.JsonProperty("courtRoomCode", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string CourtRoomCode { get; set; }

        [Newtonsoft.Json.JsonProperty("courtSittingCode", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string CourtSittingCode { get; set; }

        [Newtonsoft.Json.JsonProperty("locationId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public int? LocationId { get; set; }

        [Newtonsoft.Json.JsonProperty("isCivil", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public bool? IsCivil { get; set; }

        [Newtonsoft.Json.JsonProperty("justinOrCeisId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string JustinOrCeisId { get; set; }

        [Newtonsoft.Json.JsonProperty("estimatedTimeHour", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string EstimatedTimeHour { get; set; }

        [Newtonsoft.Json.JsonProperty("estimatedTimeMin", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string EstimatedTimeMin { get; set; }

        [Newtonsoft.Json.JsonProperty("estimatedTimeString", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string EstimatedTimeString { get; set; }

    }


    [System.CodeDom.Compiler.GeneratedCode("NJsonSchema", "14.2.0.0 (NJsonSchema v11.1.0.0 (Newtonsoft.Json v13.0.0.0))")]
    public partial class JudgeAvailability
    {
        [Newtonsoft.Json.JsonProperty("fullNm", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string FullNm { get; set; }

        [Newtonsoft.Json.JsonProperty("adjPartId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public double? AdjPartId { get; set; }

        [Newtonsoft.Json.JsonProperty("homeLocationSNm", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string HomeLocationSNm { get; set; }

        [Newtonsoft.Json.JsonProperty("availabilityCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string AvailabilityCd { get; set; }

        [Newtonsoft.Json.JsonProperty("availabilityDsc", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string AvailabilityDsc { get; set; }

        [Newtonsoft.Json.JsonProperty("date", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public System.DateTimeOffset? Date { get; set; }

        [Newtonsoft.Json.JsonProperty("dateStr", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string DateStr { get; set; }

        [Newtonsoft.Json.JsonProperty("assignmentDetails", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public System.Collections.Generic.ICollection<JudgeAssignmentDetail> AssignmentDetails { get; set; }

        [Newtonsoft.Json.JsonProperty("restrictions", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public System.Collections.Generic.ICollection<AdjudicatorRestriction> Restrictions { get; set; }

    }

    [System.CodeDom.Compiler.GeneratedCode("NJsonSchema", "14.2.0.0 (NJsonSchema v11.1.0.0 (Newtonsoft.Json v13.0.0.0))")]
    public partial class AppearanceAdjudicatorRestriction2
    {
        [Newtonsoft.Json.JsonProperty("appearanceAdjudicatorRestrictionId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public int? AppearanceAdjudicatorRestrictionId { get; set; }

        [Newtonsoft.Json.JsonProperty("hearingRestrictionId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public int? HearingRestrictionId { get; set; }

        [Newtonsoft.Json.JsonProperty("hearingRestrictionCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string HearingRestrictionCd { get; set; }

        [Newtonsoft.Json.JsonProperty("judgeId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public int? JudgeId { get; set; }

        [Newtonsoft.Json.JsonProperty("judgesInitials", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string JudgesInitials { get; set; }

        [Newtonsoft.Json.JsonProperty("fileNoTxt", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string FileNoTxt { get; set; }

        [Newtonsoft.Json.JsonProperty("hearingRestrictionTxt", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string HearingRestrictionTxt { get; set; }

        [Newtonsoft.Json.JsonProperty("hasIssue", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public bool? HasIssue { get; set; }

    }

    [System.CodeDom.Compiler.GeneratedCode("NJsonSchema", "14.2.0.0 (NJsonSchema v11.1.0.0 (Newtonsoft.Json v13.0.0.0))")]
    public partial class BestDateAppearanceDetail
    {
        [Newtonsoft.Json.JsonProperty("locationId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public int? LocationId { get; set; }

        [Newtonsoft.Json.JsonProperty("courtRoomCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string CourtRoomCd { get; set; }

        [Newtonsoft.Json.JsonProperty("appearanceId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public int? AppearanceId { get; set; }

        [Newtonsoft.Json.JsonProperty("appearanceDt", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string AppearanceDt { get; set; }

        [Newtonsoft.Json.JsonProperty("appearanceTm", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string AppearanceTm { get; set; }

        [Newtonsoft.Json.JsonProperty("courtFileNumber", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string CourtFileNumber { get; set; }

        [Newtonsoft.Json.JsonProperty("courtDivisionCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string CourtDivisionCd { get; set; }

        [Newtonsoft.Json.JsonProperty("activityCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string ActivityCd { get; set; }

        [Newtonsoft.Json.JsonProperty("activityClassCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string ActivityClassCd { get; set; }

        [Newtonsoft.Json.JsonProperty("appearanceReasonCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string AppearanceReasonCd { get; set; }

        [Newtonsoft.Json.JsonProperty("totalAppearances", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public int? TotalAppearances { get; set; }

        [Newtonsoft.Json.JsonProperty("appearanceNumber", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public int? AppearanceNumber { get; set; }

        [Newtonsoft.Json.JsonProperty("estimatedTimeHour", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string EstimatedTimeHour { get; set; }

        [Newtonsoft.Json.JsonProperty("estimatedTimeMin", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string EstimatedTimeMin { get; set; }

        [Newtonsoft.Json.JsonProperty("estimatedTimeString", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string EstimatedTimeString { get; set; }

        [Newtonsoft.Json.JsonProperty("justinNo", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string JustinNo { get; set; }

        [Newtonsoft.Json.JsonProperty("physicalFileId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string PhysicalFileId { get; set; }

        [Newtonsoft.Json.JsonProperty("appearanceAdjudicatorRestriction", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public System.Collections.Generic.ICollection<AppearanceAdjudicatorRestriction2> AppearanceAdjudicatorRestriction { get; set; }

    }

    [System.CodeDom.Compiler.GeneratedCode("NJsonSchema", "14.2.0.0 (NJsonSchema v11.1.0.0 (Newtonsoft.Json v13.0.0.0))")]
    public partial class FindBestDateResult
    {
        [Newtonsoft.Json.JsonProperty("date", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public System.DateTimeOffset? Date { get; set; }

        [Newtonsoft.Json.JsonProperty("dateStr", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string DateStr { get; set; }

        [Newtonsoft.Json.JsonProperty("capacityFlag", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public bool? CapacityFlag { get; set; }

        [Newtonsoft.Json.JsonProperty("capacityScore", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public double? CapacityScore { get; set; }

        [Newtonsoft.Json.JsonProperty("witnessFlag", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public bool? WitnessFlag { get; set; }

        [Newtonsoft.Json.JsonProperty("witnessScore", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public double? WitnessScore { get; set; }

        [Newtonsoft.Json.JsonProperty("judgeFlag", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public bool? JudgeFlag { get; set; }

        [Newtonsoft.Json.JsonProperty("additionsAllowedFlag", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public bool? AdditionsAllowedFlag { get; set; }

        [Newtonsoft.Json.JsonProperty("closedComments", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string ClosedComments { get; set; }

        [Newtonsoft.Json.JsonProperty("wrongDurationFlag", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public bool? WrongDurationFlag { get; set; }

        [Newtonsoft.Json.JsonProperty("counselAvailabilityFlag", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public bool? CounselAvailabilityFlag { get; set; }

        [Newtonsoft.Json.JsonProperty("crownCounselAvailabilityFlag", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public bool? CrownCounselAvailabilityFlag { get; set; }

        [Newtonsoft.Json.JsonProperty("bestDateFlag", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public bool? BestDateFlag { get; set; }

        [Newtonsoft.Json.JsonProperty("totalQuantity", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public double? TotalQuantity { get; set; }

        [Newtonsoft.Json.JsonProperty("usedQuantity", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public double? UsedQuantity { get; set; }

        [Newtonsoft.Json.JsonProperty("numberOfCases", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public int? NumberOfCases { get; set; }

        [Newtonsoft.Json.JsonProperty("numberOfHours", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public double? NumberOfHours { get; set; }

        [Newtonsoft.Json.JsonProperty("personnelAvailability", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public System.Collections.Generic.ICollection<PersonnelAvailability> PersonnelAvailability { get; set; }

        [Newtonsoft.Json.JsonProperty("activityClassUsages", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public System.Collections.Generic.ICollection<ActivityClassUsage> ActivityClassUsages { get; set; }

        [Newtonsoft.Json.JsonProperty("availabilityCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string AvailabilityCd { get; set; }

        [Newtonsoft.Json.JsonProperty("activityCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string ActivityCd { get; set; }

        [Newtonsoft.Json.JsonProperty("activityCdDsc", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string ActivityCdDsc { get; set; }

        [Newtonsoft.Json.JsonProperty("courtRoomCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string CourtRoomCd { get; set; }

        [Newtonsoft.Json.JsonProperty("capacityConstraintCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string CapacityConstraintCd { get; set; }

        [Newtonsoft.Json.JsonProperty("offeredDates", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public System.Collections.Generic.ICollection<OfferedDate2> OfferedDates { get; set; }

        [Newtonsoft.Json.JsonProperty("counselAvailability", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public System.Collections.Generic.ICollection<CounselAvailability> CounselAvailability { get; set; }

        [Newtonsoft.Json.JsonProperty("judgeAvailability", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public JudgeAvailability JudgeAvailability { get; set; }

        [Newtonsoft.Json.JsonProperty("restrictions", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public System.Collections.Generic.ICollection<AdjudicatorRestriction> Restrictions { get; set; }

        [Newtonsoft.Json.JsonProperty("hasRestrictions", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public bool? HasRestrictions { get; set; }

        [Newtonsoft.Json.JsonProperty("hasAdjudicatorIssues", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public bool? HasAdjudicatorIssues { get; set; }

        [Newtonsoft.Json.JsonProperty("appearances", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public System.Collections.Generic.ICollection<BestDateAppearanceDetail> Appearances { get; set; }

        [Newtonsoft.Json.JsonProperty("completedPersonnelSearch", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public bool? CompletedPersonnelSearch { get; set; }

    }

    [System.CodeDom.Compiler.GeneratedCode("NJsonSchema", "14.2.0.0 (NJsonSchema v11.1.0.0 (Newtonsoft.Json v13.0.0.0))")]
    public partial class CourtActivityDetail
    {
        [Newtonsoft.Json.JsonProperty("courtActivityId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public int? CourtActivityId { get; set; }

        [Newtonsoft.Json.JsonProperty("noAdditionsYn", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string NoAdditionsYn { get; set; }

        [Newtonsoft.Json.JsonProperty("noAdditionsCommentTxt", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string NoAdditionsCommentTxt { get; set; }

        [Newtonsoft.Json.JsonProperty("courtSittingCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string CourtSittingCd { get; set; }

    }

    [System.CodeDom.Compiler.GeneratedCode("NJsonSchema", "14.2.0.0 (NJsonSchema v11.1.0.0 (Newtonsoft.Json v13.0.0.0))")]
    public partial class AdjudicatorDetail
    {
        [Newtonsoft.Json.JsonProperty("adjudicatorId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public int? AdjudicatorId { get; set; }

        [Newtonsoft.Json.JsonProperty("adjudicatorNm", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string AdjudicatorNm { get; set; }

        [Newtonsoft.Json.JsonProperty("adjudicatorInitials", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string AdjudicatorInitials { get; set; }

        [Newtonsoft.Json.JsonProperty("amPm", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string AmPm { get; set; }

    }

    [System.CodeDom.Compiler.GeneratedCode("NJsonSchema", "14.2.0.0 (NJsonSchema v11.1.0.0 (Newtonsoft.Json v13.0.0.0))")]
    public partial class CourtRoomDetail
    {
        [Newtonsoft.Json.JsonProperty("courtRoomCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string CourtRoomCd { get; set; }

        [Newtonsoft.Json.JsonProperty("assignmentListRoomYn", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string AssignmentListRoomYn { get; set; }

        [Newtonsoft.Json.JsonProperty("casesTarget", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public int? CasesTarget { get; set; }

        [Newtonsoft.Json.JsonProperty("totalHours", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public double? TotalHours { get; set; }

        [Newtonsoft.Json.JsonProperty("isAM", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string IsAM { get; set; }

        [Newtonsoft.Json.JsonProperty("isPM", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string IsPM { get; set; }

        [Newtonsoft.Json.JsonProperty("adjudicatorDetails", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public System.Collections.Generic.ICollection<AdjudicatorDetail> AdjudicatorDetails { get; set; }

    }

    [System.CodeDom.Compiler.GeneratedCode("NJsonSchema", "14.2.0.0 (NJsonSchema v11.1.0.0 (Newtonsoft.Json v13.0.0.0))")]
    public partial class ActivityAppearanceResults
    {
        [Newtonsoft.Json.JsonProperty("dateStr", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string DateStr { get; set; }

        [Newtonsoft.Json.JsonProperty("locationId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public int? LocationId { get; set; }

        [Newtonsoft.Json.JsonProperty("locationNm", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string LocationNm { get; set; }

        [Newtonsoft.Json.JsonProperty("activityCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string ActivityCd { get; set; }

        [Newtonsoft.Json.JsonProperty("activityDsc", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string ActivityDsc { get; set; }

        [Newtonsoft.Json.JsonProperty("activityClassCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string ActivityClassCd { get; set; }

        [Newtonsoft.Json.JsonProperty("activityClassDsc", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string ActivityClassDsc { get; set; }

        [Newtonsoft.Json.JsonProperty("courtRooms", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public System.Collections.Generic.ICollection<string> CourtRooms { get; set; }

        [Newtonsoft.Json.JsonProperty("capacityTargetNumerator", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public double? CapacityTargetNumerator { get; set; }

        [Newtonsoft.Json.JsonProperty("capacityTargetDenominator", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public double? CapacityTargetDenominator { get; set; }

        [Newtonsoft.Json.JsonProperty("casesTarget", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public int? CasesTarget { get; set; }

        [Newtonsoft.Json.JsonProperty("totalHours", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public double? TotalHours { get; set; }

        [Newtonsoft.Json.JsonProperty("capacityConstraintCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string CapacityConstraintCd { get; set; }

        [Newtonsoft.Json.JsonProperty("capacityConstraintDsc", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string CapacityConstraintDsc { get; set; }

        [Newtonsoft.Json.JsonProperty("appearances", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public System.Collections.Generic.ICollection<ActivityAppearanceDetail> Appearances { get; set; }

        [Newtonsoft.Json.JsonProperty("courtActivityDetails", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public System.Collections.Generic.ICollection<CourtActivityDetail> CourtActivityDetails { get; set; }

        [Newtonsoft.Json.JsonProperty("courtRoomDetails", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public System.Collections.Generic.ICollection<CourtRoomDetail> CourtRoomDetails { get; set; }

    }

    [System.CodeDom.Compiler.GeneratedCode("NJsonSchema", "14.2.0.0 (NJsonSchema v11.1.0.0 (Newtonsoft.Json v13.0.0.0))")]
    public partial class ActivityAppearanceResultsCollection
    {
        [Newtonsoft.Json.JsonProperty("items", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public System.Collections.Generic.ICollection<ActivityAppearanceResults> Items { get; set; }

        [Newtonsoft.Json.JsonProperty("isCourtListFiltered", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public bool? IsCourtListFiltered { get; set; }

        [Newtonsoft.Json.JsonProperty("isStat", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public bool? IsStat { get; set; }

    }

    [System.CodeDom.Compiler.GeneratedCode("NSwag", "14.2.0.0 (NJsonSchema v11.1.0.0 (Newtonsoft.Json v13.0.0.0))")]
    public partial class ApiException : System.Exception
    {
        public int StatusCode { get; private set; }

        public string Response { get; private set; }

        public System.Collections.Generic.IReadOnlyDictionary<string, System.Collections.Generic.IEnumerable<string>> Headers { get; private set; }

        public ApiException(string message, int statusCode, string response, System.Collections.Generic.IReadOnlyDictionary<string, System.Collections.Generic.IEnumerable<string>> headers, System.Exception innerException)
            : base(message + "\n\nStatus: " + statusCode + "\nResponse: \n" + ((response == null) ? "(null)" : response.Substring(0, response.Length >= 512 ? 512 : response.Length)), innerException)
        {
            StatusCode = statusCode;
            Response = response;
            Headers = headers;
        }

        public override string ToString()
        {
            return string.Format("HTTP Response: \n\n{0}\n\n{1}", Response, base.ToString());
        }
    }

    [System.CodeDom.Compiler.GeneratedCode("NJsonSchema", "14.2.0.0 (NJsonSchema v11.1.0.0 (Newtonsoft.Json v13.0.0.0))")]
    public partial class RequiredPerson
    {
        [Newtonsoft.Json.JsonProperty("partId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string PartId { get; set; }

        [Newtonsoft.Json.JsonProperty("personTypeCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string PersonTypeCd { get; set; }

    }

    [System.CodeDom.Compiler.GeneratedCode("NJsonSchema", "14.2.0.0 (NJsonSchema v11.1.0.0 (Newtonsoft.Json v13.0.0.0))")]
    public partial class FindBestDateParameters
    {
        [Newtonsoft.Json.JsonProperty("locationId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public int? LocationId { get; set; }

        [Newtonsoft.Json.JsonProperty("adjPartId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public double? AdjPartId { get; set; }

        [Newtonsoft.Json.JsonProperty("estimatedQty", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public double? EstimatedQty { get; set; }

        [Newtonsoft.Json.JsonProperty("estimatedUnitCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string EstimatedUnitCd { get; set; }

        [Newtonsoft.Json.JsonProperty("startDate", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string StartDate { get; set; }

        [Newtonsoft.Json.JsonProperty("endDate", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string EndDate { get; set; }

        [Newtonsoft.Json.JsonProperty("justinNo", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public int? JustinNo { get; set; }

        [Newtonsoft.Json.JsonProperty("physicalFileId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public double? PhysicalFileId { get; set; }

        [Newtonsoft.Json.JsonProperty("requiredPersonnel", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public System.Collections.Generic.ICollection<RequiredPerson> RequiredPersonnel { get; set; }

        [Newtonsoft.Json.JsonProperty("counselIds", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public System.Collections.Generic.ICollection<int> CounselIds { get; set; }

        [Newtonsoft.Json.JsonProperty("activityCds", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public System.Collections.Generic.ICollection<string> ActivityCds { get; set; }

        [Newtonsoft.Json.JsonProperty("bestDateYn", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string BestDateYn { get; set; }

        [Newtonsoft.Json.JsonProperty("includeAppearancesYn", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string IncludeAppearancesYn { get; set; }

    }

    [System.CodeDom.Compiler.GeneratedCode("NJsonSchema", "14.2.0.0 (NJsonSchema v11.1.0.0 (Newtonsoft.Json v13.0.0.0))")]
    public partial class AppearanceMethodDetail
    {
        [Newtonsoft.Json.JsonProperty("appearanceId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string AppearanceId { get; set; }

        [Newtonsoft.Json.JsonProperty("roleTypeCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string RoleTypeCd { get; set; }

        [Newtonsoft.Json.JsonProperty("appearanceMethodCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string AppearanceMethodCd { get; set; }

        [Newtonsoft.Json.JsonProperty("assetUsageSeqNo", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string AssetUsageSeqNo { get; set; }

        [Newtonsoft.Json.JsonProperty("phoneNumberTxt", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string PhoneNumberTxt { get; set; }

        [Newtonsoft.Json.JsonProperty("instructionTxt", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string InstructionTxt { get; set; }

        [Newtonsoft.Json.JsonProperty("apprMethodCcn", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string ApprMethodCcn { get; set; }

        [Newtonsoft.Json.JsonProperty("origRoleCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string OrigRoleCd { get; set; }

        [Newtonsoft.Json.JsonProperty("origAppearanceMethodCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string OrigAppearanceMethodCd { get; set; }

    }

    [System.CodeDom.Compiler.GeneratedCode("NJsonSchema", "14.2.0.0 (NJsonSchema v11.1.0.0 (Newtonsoft.Json v13.0.0.0))")]
    public partial class AppearanceMethod
    {
        [Newtonsoft.Json.JsonProperty("responseMessageTxt", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string ResponseMessageTxt { get; set; }

        [Newtonsoft.Json.JsonProperty("responseCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string ResponseCd { get; set; }

        [Newtonsoft.Json.JsonProperty("courtDivisionCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string CourtDivisionCd { get; set; }

        [Newtonsoft.Json.JsonProperty("details", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public System.Collections.Generic.ICollection<AppearanceMethodDetail> Details { get; set; }

    }

    [System.CodeDom.Compiler.GeneratedCode("NJsonSchema", "14.2.0.0 (NJsonSchema v11.1.0.0 (Newtonsoft.Json v13.0.0.0))")]
    public partial class EquipmentBooking
    {
        [Newtonsoft.Json.JsonProperty("appearanceId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string AppearanceId { get; set; }

        [Newtonsoft.Json.JsonProperty("courtDivisionCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string CourtDivisionCd { get; set; }

        [Newtonsoft.Json.JsonProperty("resourceId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string ResourceId { get; set; }

        [Newtonsoft.Json.JsonProperty("bookingDt", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string BookingDt { get; set; }

        [Newtonsoft.Json.JsonProperty("bookingFromTm", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string BookingFromTm { get; set; }

        [Newtonsoft.Json.JsonProperty("bookingToTm", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string BookingToTm { get; set; }

        [Newtonsoft.Json.JsonProperty("locationId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public int? LocationId { get; set; }

        [Newtonsoft.Json.JsonProperty("locationNm", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string LocationNm { get; set; }

        [Newtonsoft.Json.JsonProperty("courtRoomCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string CourtRoomCd { get; set; }

        [Newtonsoft.Json.JsonProperty("bookingCommentTxt", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string BookingCommentTxt { get; set; }

        [Newtonsoft.Json.JsonProperty("courtFileNumberTxt", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string CourtFileNumberTxt { get; set; }

        [Newtonsoft.Json.JsonProperty("bookedByNm", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string BookedByNm { get; set; }

        [Newtonsoft.Json.JsonProperty("bookingId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string BookingId { get; set; }

        [Newtonsoft.Json.JsonProperty("bookingCcn", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string BookingCcn { get; set; }

        [Newtonsoft.Json.JsonProperty("responseMessageTxt", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string ResponseMessageTxt { get; set; }

        [Newtonsoft.Json.JsonProperty("responseCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string ResponseCd { get; set; }

    }

    [System.CodeDom.Compiler.GeneratedCode("NJsonSchema", "14.2.0.0 (NJsonSchema v11.1.0.0 (Newtonsoft.Json v13.0.0.0))")]
    public partial class Equipment
    {
        [Newtonsoft.Json.JsonProperty("locationId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public int? LocationId { get; set; }

        [Newtonsoft.Json.JsonProperty("resourceId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string ResourceId { get; set; }

        [Newtonsoft.Json.JsonProperty("resourceNm", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string ResourceNm { get; set; }

        [Newtonsoft.Json.JsonProperty("assetTypeCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string AssetTypeCd { get; set; }

        [Newtonsoft.Json.JsonProperty("assetUsageRuleCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string AssetUsageRuleCd { get; set; }

        [Newtonsoft.Json.JsonProperty("commentTxt", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string CommentTxt { get; set; }

        [Newtonsoft.Json.JsonProperty("phoneNumberTxt", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string PhoneNumberTxt { get; set; }

        [Newtonsoft.Json.JsonProperty("availableRooms", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public System.Collections.Generic.ICollection<string> AvailableRooms { get; set; }

        [Newtonsoft.Json.JsonProperty("equipmentBookings", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public System.Collections.Generic.ICollection<EquipmentBooking> EquipmentBookings { get; set; }

    }

    [System.CodeDom.Compiler.GeneratedCode("NJsonSchema", "14.2.0.0 (NJsonSchema v11.1.0.0 (Newtonsoft.Json v13.0.0.0))")]
    public partial class EquipmentSearchResults
    {
        [Newtonsoft.Json.JsonProperty("appearanceId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string AppearanceId { get; set; }

        [Newtonsoft.Json.JsonProperty("responseMessageTxt", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string ResponseMessageTxt { get; set; }

        [Newtonsoft.Json.JsonProperty("responseCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string ResponseCd { get; set; }

        [Newtonsoft.Json.JsonProperty("primaryResource", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public System.Collections.Generic.ICollection<Equipment> PrimaryResource { get; set; }

        [Newtonsoft.Json.JsonProperty("secondaryResource", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public System.Collections.Generic.ICollection<Equipment> SecondaryResource { get; set; }

    }

    [System.CodeDom.Compiler.GeneratedCode("NJsonSchema", "14.2.0.0 (NJsonSchema v11.1.0.0 (Newtonsoft.Json v13.0.0.0))")]
    public partial class Adjudicator
    {
        [Newtonsoft.Json.JsonProperty("judiciaryPersonId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public int? JudiciaryPersonId { get; set; }

        [Newtonsoft.Json.JsonProperty("partID", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public double? PartID { get; set; }

        [Newtonsoft.Json.JsonProperty("adjudicatorNm", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string AdjudicatorNm { get; set; }

        [Newtonsoft.Json.JsonProperty("adjudicatorInitials", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string AdjudicatorInitials { get; set; }

        [Newtonsoft.Json.JsonProperty("locationId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string LocationId { get; set; }

    }

    [System.CodeDom.Compiler.GeneratedCode("NJsonSchema", "14.2.0.0 (NJsonSchema v11.1.0.0 (Newtonsoft.Json v13.0.0.0))")]
    public partial class AslChildAppearance
    {
        [Newtonsoft.Json.JsonProperty("adjudicatorInitials", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string AdjudicatorInitials { get; set; }

        [Newtonsoft.Json.JsonProperty("adjudicatorNm", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string AdjudicatorNm { get; set; }

        [Newtonsoft.Json.JsonProperty("courtRoomCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string CourtRoomCd { get; set; }

        [Newtonsoft.Json.JsonProperty("appearanceDt", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string AppearanceDt { get; set; }

        [Newtonsoft.Json.JsonProperty("appearanceTm", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string AppearanceTm { get; set; }

    }

    [System.CodeDom.Compiler.GeneratedCode("NJsonSchema", "14.2.0.0 (NJsonSchema v11.1.0.0 (Newtonsoft.Json v13.0.0.0))")]
    public partial class Charge
    {
        [Newtonsoft.Json.JsonProperty("sectionTxt", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string SectionTxt { get; set; }

        [Newtonsoft.Json.JsonProperty("sectionDscTxt", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string SectionDscTxt { get; set; }

    }

    [System.CodeDom.Compiler.GeneratedCode("NJsonSchema", "14.2.0.0 (NJsonSchema v11.1.0.0 (Newtonsoft.Json v13.0.0.0))")]
    public partial class Crown
    {
        [Newtonsoft.Json.JsonProperty("partId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string PartId { get; set; }

        [Newtonsoft.Json.JsonProperty("lastNm", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string LastNm { get; set; }

        [Newtonsoft.Json.JsonProperty("givenNm", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string GivenNm { get; set; }

        [Newtonsoft.Json.JsonProperty("assignedYn", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string AssignedYn { get; set; }

    }

    [System.CodeDom.Compiler.GeneratedCode("NJsonSchema", "14.2.0.0 (NJsonSchema v11.1.0.0 (Newtonsoft.Json v13.0.0.0))")]
    public partial class PcssCounsel
    {
        [Newtonsoft.Json.JsonProperty("counselId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public int? CounselId { get; set; }

        [Newtonsoft.Json.JsonProperty("lawSocietyId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public int? LawSocietyId { get; set; }

        [Newtonsoft.Json.JsonProperty("lastNm", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string LastNm { get; set; }

        [Newtonsoft.Json.JsonProperty("givenNm", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string GivenNm { get; set; }

        [Newtonsoft.Json.JsonProperty("prefNm", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string PrefNm { get; set; }

        [Newtonsoft.Json.JsonProperty("addressLine1Txt", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string AddressLine1Txt { get; set; }

        [Newtonsoft.Json.JsonProperty("addressLine2Txt", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string AddressLine2Txt { get; set; }

        [Newtonsoft.Json.JsonProperty("cityTxt", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string CityTxt { get; set; }

        [Newtonsoft.Json.JsonProperty("province", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string Province { get; set; }

        [Newtonsoft.Json.JsonProperty("postalCode", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string PostalCode { get; set; }

        [Newtonsoft.Json.JsonProperty("phoneNoTxt", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string PhoneNoTxt { get; set; }

        [Newtonsoft.Json.JsonProperty("emailAddressTxt", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string EmailAddressTxt { get; set; }

        [Newtonsoft.Json.JsonProperty("activeYn", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string ActiveYn { get; set; }

        [Newtonsoft.Json.JsonProperty("counselType", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string CounselType { get; set; }

        [Newtonsoft.Json.JsonProperty("orgNm", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
        public string OrgNm { get; set; }

        [System.CodeDom.Compiler.GeneratedCode("NJsonSchema", "14.2.0.0 (NJsonSchema v11.1.0.0 (Newtonsoft.Json v13.0.0.0))")]
        public partial class JustinCounsel
        {
            [Newtonsoft.Json.JsonProperty("lastNm", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string LastNm { get; set; }

            [Newtonsoft.Json.JsonProperty("givenNm", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string GivenNm { get; set; }

            [Newtonsoft.Json.JsonProperty("counselEnteredDt", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string CounselEnteredDt { get; set; }

            [Newtonsoft.Json.JsonProperty("counselPartId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string CounselPartId { get; set; }

            [Newtonsoft.Json.JsonProperty("counselRelatedRepTypeCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string CounselRelatedRepTypeCd { get; set; }

            [Newtonsoft.Json.JsonProperty("counselRrepId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string CounselRrepId { get; set; }

        }

        [System.CodeDom.Compiler.GeneratedCode("NJsonSchema", "14.2.0.0 (NJsonSchema v11.1.0.0 (Newtonsoft.Json v13.0.0.0))")]
        public partial class JcmComment2
        {
            [Newtonsoft.Json.JsonProperty("jcmCommentId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public int? JcmCommentId { get; set; }

            [Newtonsoft.Json.JsonProperty("justinNo", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string JustinNo { get; set; }

            [Newtonsoft.Json.JsonProperty("physicalFileId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string PhysicalFileId { get; set; }

            [Newtonsoft.Json.JsonProperty("commentTxt", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string CommentTxt { get; set; }

            [Newtonsoft.Json.JsonProperty("entDtm", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string EntDtm { get; set; }

            [Newtonsoft.Json.JsonProperty("updDtm", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string UpdDtm { get; set; }

            [Newtonsoft.Json.JsonProperty("rotaInitialsCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string RotaInitialsCd { get; set; }

            [Newtonsoft.Json.JsonProperty("fullName", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string FullName { get; set; }

        }

        [System.CodeDom.Compiler.GeneratedCode("NJsonSchema", "14.2.0.0 (NJsonSchema v11.1.0.0 (Newtonsoft.Json v13.0.0.0))")]
        public partial class ActivityAppearanceDetail
        {
            [Newtonsoft.Json.JsonProperty("aslSortOrder", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public int? AslSortOrder { get; set; }

            [Newtonsoft.Json.JsonProperty("courtDivisionCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string CourtDivisionCd { get; set; }

            [Newtonsoft.Json.JsonProperty("appearanceDt", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string AppearanceDt { get; set; }

            [Newtonsoft.Json.JsonProperty("appearanceTm", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string AppearanceTm { get; set; }

            [Newtonsoft.Json.JsonProperty("courtRoomCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string CourtRoomCd { get; set; }

            [Newtonsoft.Json.JsonProperty("courtFileNumber", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string CourtFileNumber { get; set; }

            [Newtonsoft.Json.JsonProperty("pcssAppearanceId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public int? PcssAppearanceId { get; set; }

            [Newtonsoft.Json.JsonProperty("isComplete", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public bool? IsComplete { get; set; }

            [Newtonsoft.Json.JsonProperty("activityClassCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string ActivityClassCd { get; set; }

            [Newtonsoft.Json.JsonProperty("activityClassDsc", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string ActivityClassDsc { get; set; }

            [Newtonsoft.Json.JsonProperty("appearanceReasonCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string AppearanceReasonCd { get; set; }

            [Newtonsoft.Json.JsonProperty("appearanceReasonDsc", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string AppearanceReasonDsc { get; set; }

            [Newtonsoft.Json.JsonProperty("appearanceMethod", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public AppearanceMethod AppearanceMethod { get; set; }

            [Newtonsoft.Json.JsonProperty("equipmentBooking", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public EquipmentSearchResults EquipmentBooking { get; set; }

            [Newtonsoft.Json.JsonProperty("scheduleNoteTxt", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string ScheduleNoteTxt { get; set; }

            [Newtonsoft.Json.JsonProperty("estimatedTimeHour", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string EstimatedTimeHour { get; set; }

            [Newtonsoft.Json.JsonProperty("estimatedTimeMin", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string EstimatedTimeMin { get; set; }

            [Newtonsoft.Json.JsonProperty("estimatedTimeString", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string EstimatedTimeString { get; set; }

            [Newtonsoft.Json.JsonProperty("justinNo", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string JustinNo { get; set; }

            [Newtonsoft.Json.JsonProperty("physicalFileId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string PhysicalFileId { get; set; }

            [Newtonsoft.Json.JsonProperty("courtlistRefNumber", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string CourtlistRefNumber { get; set; }

            [Newtonsoft.Json.JsonProperty("styleOfCause", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string StyleOfCause { get; set; }

            [Newtonsoft.Json.JsonProperty("adjudicatorInitials", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string AdjudicatorInitials { get; set; }

            [Newtonsoft.Json.JsonProperty("adjudicatorNm", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string AdjudicatorNm { get; set; }

            [Newtonsoft.Json.JsonProperty("caseAgeDays", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string CaseAgeDays { get; set; }

            [Newtonsoft.Json.JsonProperty("videoYn", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string VideoYn { get; set; }

            [Newtonsoft.Json.JsonProperty("accusedNm", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string AccusedNm { get; set; }

            [Newtonsoft.Json.JsonProperty("accusedCounselNm", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string AccusedCounselNm { get; set; }

            [Newtonsoft.Json.JsonProperty("appearanceId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string AppearanceId { get; set; }

            [Newtonsoft.Json.JsonProperty("profPartId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string ProfPartId { get; set; }

            [Newtonsoft.Json.JsonProperty("profSeqNo", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string ProfSeqNo { get; set; }

            [Newtonsoft.Json.JsonProperty("inCustodyYn", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string InCustodyYn { get; set; }

            [Newtonsoft.Json.JsonProperty("detainedYn", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string DetainedYn { get; set; }

            [Newtonsoft.Json.JsonProperty("continuationYn", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string ContinuationYn { get; set; }

            [Newtonsoft.Json.JsonProperty("condSentenceOrderYn", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string CondSentenceOrderYn { get; set; }

            [Newtonsoft.Json.JsonProperty("lackCourtTimeYn", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string LackCourtTimeYn { get; set; }

            [Newtonsoft.Json.JsonProperty("otherFactorsYn", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string OtherFactorsYn { get; set; }

            [Newtonsoft.Json.JsonProperty("otherFactorsComment", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string OtherFactorsComment { get; set; }

            [Newtonsoft.Json.JsonProperty("cfcsaYn", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string CfcsaYn { get; set; }

            [Newtonsoft.Json.JsonProperty("softYn", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string SoftYn { get; set; }

            [Newtonsoft.Json.JsonProperty("scheduledOnDt", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string ScheduledOnDt { get; set; }

            [Newtonsoft.Json.JsonProperty("scheduledByInitials", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string ScheduledByInitials { get; set; }

            [Newtonsoft.Json.JsonProperty("scheduledByName", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string ScheduledByName { get; set; }

            [Newtonsoft.Json.JsonProperty("activityCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string ActivityCd { get; set; }

            [Newtonsoft.Json.JsonProperty("activityDsc", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string ActivityDsc { get; set; }

            [Newtonsoft.Json.JsonProperty("courtActivityId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public int? CourtActivityId { get; set; }

            [Newtonsoft.Json.JsonProperty("courtActivitySlotId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public int? CourtActivitySlotId { get; set; }

            [Newtonsoft.Json.JsonProperty("remoteVideoYn", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string RemoteVideoYn { get; set; }

            [Newtonsoft.Json.JsonProperty("appearanceStatusCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string AppearanceStatusCd { get; set; }

            [Newtonsoft.Json.JsonProperty("appearanceStatusDsc", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string AppearanceStatusDsc { get; set; }

            [Newtonsoft.Json.JsonProperty("totalAppearances", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public int? TotalAppearances { get; set; }

            [Newtonsoft.Json.JsonProperty("appearanceNumber", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public int? AppearanceNumber { get; set; }

            [Newtonsoft.Json.JsonProperty("trialTrackerCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string TrialTrackerCd { get; set; }

            [Newtonsoft.Json.JsonProperty("trialTrackerDsc", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string TrialTrackerDsc { get; set; }

            [Newtonsoft.Json.JsonProperty("trialTrackerTrialResultTxt", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string TrialTrackerTrialResultTxt { get; set; }

            [Newtonsoft.Json.JsonProperty("trialTrackerOtherTxt", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string TrialTrackerOtherTxt { get; set; }

            [Newtonsoft.Json.JsonProperty("aslParentTrialTrackerCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string AslParentTrialTrackerCd { get; set; }

            [Newtonsoft.Json.JsonProperty("aslParentTrialTrackerDsc", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string AslParentTrialTrackerDsc { get; set; }

            [Newtonsoft.Json.JsonProperty("assignmentListRoomYn", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string AssignmentListRoomYn { get; set; }

            [Newtonsoft.Json.JsonProperty("aslChildAppearance", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public AslChildAppearance AslChildAppearance { get; set; }

            [Newtonsoft.Json.JsonProperty("charges", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public System.Collections.Generic.ICollection<Charge> Charges { get; set; }

            [Newtonsoft.Json.JsonProperty("crown", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public System.Collections.Generic.ICollection<Crown> Crown { get; set; }

            [Newtonsoft.Json.JsonProperty("counsel", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public System.Collections.Generic.ICollection<PcssCounsel> Counsel { get; set; }

            [Newtonsoft.Json.JsonProperty("justinCounsel", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public JustinCounsel JustinCounsel { get; set; }

            [Newtonsoft.Json.JsonProperty("homeLocationId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public int? HomeLocationId { get; set; }

            [Newtonsoft.Json.JsonProperty("homeLocationNm", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string HomeLocationNm { get; set; }

            [Newtonsoft.Json.JsonProperty("remoteLocationId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public int? RemoteLocationId { get; set; }

            [Newtonsoft.Json.JsonProperty("remoteLocationNm", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string RemoteLocationNm { get; set; }

            [Newtonsoft.Json.JsonProperty("ceisCounsel", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public object CeisCounsel { get; set; }

            [Newtonsoft.Json.JsonProperty("justinApprId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string JustinApprId { get; set; }

            [Newtonsoft.Json.JsonProperty("ceisAppearanceId", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string CeisAppearanceId { get; set; }

            [Newtonsoft.Json.JsonProperty("jcmComments", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public System.Collections.Generic.ICollection<JcmComment2> JcmComments { get; set; }

            [Newtonsoft.Json.JsonProperty("appearanceAdjudicatorRestriction", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public System.Collections.Generic.ICollection<AppearanceAdjudicatorRestriction2> AppearanceAdjudicatorRestriction { get; set; }

            [Newtonsoft.Json.JsonProperty("stoodDownJCMYn", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string StoodDownJCMYn { get; set; }

            [Newtonsoft.Json.JsonProperty("courtClassCd", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string CourtClassCd { get; set; }

            [Newtonsoft.Json.JsonProperty("appearanceSequenceNumber", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string AppearanceSequenceNumber { get; set; }

            [Newtonsoft.Json.JsonProperty("fixedListDoneYn", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string FixedListDoneYn { get; set; }

            [Newtonsoft.Json.JsonProperty("aslCourtFileNumber", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string AslCourtFileNumber { get; set; }

            [Newtonsoft.Json.JsonProperty("selfRepresentedYn", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string SelfRepresentedYn { get; set; }

            [Newtonsoft.Json.JsonProperty("otherRepresentedYn", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public string OtherRepresentedYn { get; set; }

            [Newtonsoft.Json.JsonProperty("linkedCounsel", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public PcssCounsel LinkedCounsel { get; set; }

            [Newtonsoft.Json.JsonProperty("aslFeederAdjudicators", Required = Newtonsoft.Json.Required.Default, NullValueHandling = Newtonsoft.Json.NullValueHandling.Ignore)]
            public System.Collections.Generic.ICollection<Adjudicator> AslFeederAdjudicators { get; set; }
        }
    }
}