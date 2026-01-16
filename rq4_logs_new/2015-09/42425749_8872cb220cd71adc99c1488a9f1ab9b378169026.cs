using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using System.IO;
using System.Net;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;
using yodleemodel;

namespace yodlee.pureprofile.com.Controllers
{

    public class HomeController : Controller
    {
        OAuth oauth_obj = new OAuth();

        string fastlinkURL = "https://fastlink.yodlee.com/appscenter/fastlinksb/linkAccount.fastlinksb.action";

        string consumerKey = "a458bdf184d34c0cab7ef7ffbb5f016b";
        string consumerSecret = "1ece74e1ca9e4befbb1b64daba7c4a24";
        string bridgetAppId = "10003200";

        //string strCobSessionToken = "08062013_0:0bf656a35fb85d99098b5df977a4db6ba425e06dc81aef61fdc64144ee978850c8460f71a0fe132ad6d8ff040be155c311e7d1eaf9f36d98dfb0ba11fa195fda";
        //string strUserSessionToken = "08062013_0:a3b40a5eba8cd9da11faf7406528aeab7a9bccd2a7fb54b65f423b4a0b894f40af2ff9ccb61ee7c1d8268fb3036c06e87feb4fbaf29bffdbd236e0bf074d7d0f";
        string strYodleeDomain = "https://rest.developer.yodlee.com";

        public ActionResult Index()
        {
            try
            {
                // step 1 ->login the cobrand using the cob login api
                string strCobURL = strYodleeDomain + "/services/srest/restserver/v1.0/authenticate/coblogin";
                string coburl = strCobURL + "?cobrandLogin=sbCobUmang&cobrandPassword=5e52e456-aa9a-465e-8e2f-f91fa8f01c1a";
                string cobloginresponse = WebRequest(WebMethod.POST, coburl, "", null);
                cobLoginAuthDetails _Cobdata = new cobLoginAuthDetails();
                _Cobdata = Newtonsoft.Json.JsonConvert.DeserializeObject<cobLoginAuthDetails>(cobloginresponse);

                // step 2 -> call the getRefreshedUserItems api for the previous minute
                /*  string strRefreshedUserItemsData = GetRefreshedUserItems(_Cobdata.cobrandConversationCredentials.sessionToken);
                  List<RefreshedUserItemsModel> lstRefreshedUserItemsModel = new List<RefreshedUserItemsModel>();
                  lstRefreshedUserItemsModel = JsonConvert.DeserializeObject<List<RefreshedUserItemsModel>>(strRefreshedUserItemsData);

                  // step 3 -> loop through each item that was refreshed and extract the appropriate data
                  for (int i = 0; i < lstRefreshedUserItemsModel.Count; i++)
                  {
                      string strLoginName = lstRefreshedUserItemsModel[i].loginName;
                  }*/

                // user login
                string struserloginURL = strYodleeDomain + "/services/srest/restserver/v1.0/authenticate/login";
                string loginurl = struserloginURL + "?cobSessionToken=" + Server.UrlEncode(_Cobdata.cobrandConversationCredentials.sessionToken) + "&login=sbMemUmang1&password=" + Server.UrlEncode("sbMemUmang1#123") + "";

                userLoginAuthDetails _Userdata = new userLoginAuthDetails();

                var codelogin = new
                {
                    cobSessionToken = _Cobdata.cobrandConversationCredentials.sessionToken,
                    login = "sbMemUmang1",
                    password = "sbMemUmang1#123"
                };

                string _codelogin = Newtonsoft.Json.JsonConvert.SerializeObject(codelogin);
                string userloginresponse = WebRequest(WebMethod.POST, loginurl, "", null);
                _Userdata = Newtonsoft.Json.JsonConvert.DeserializeObject<userLoginAuthDetails>(userloginresponse);

                // getOAuthAccessToken
                string authURL = strYodleeDomain + "/services/srest/restserver/v1.0/jsonsdk/OAuthAccessTokenManagementService/getOAuthAccessToken";
                authURL = authURL + "?cobSessionToken=" + Server.UrlEncode(_Cobdata.cobrandConversationCredentials.sessionToken) + "&userSessionToken=" + Server.UrlEncode(_Userdata.userContext.conversationCredentials.sessionToken) + "&bridgetAppId=" + bridgetAppId + "";
                string authresponse = WebRequest(WebMethod.POST, authURL, "", null);
                authTokens _data = new authTokens();

                _data = Newtonsoft.Json.JsonConvert.DeserializeObject<authTokens>(authresponse);
                string nurl = "", normalizerequestparameter = "", timespamp = "", nonse = "";
                string strVal = string.Empty;

                while (string.IsNullOrWhiteSpace(strVal) || strVal.Contains("+"))
                {
                    strVal = oauth_obj.GetSignature(consumerKey, consumerSecret, _data.token, _data.tokenSecret, OAuth.WebMethod.GET, fastlinkURL, out timespamp, out nonse, out nurl, out normalizerequestparameter, "developer.yodlee.com", "");
                }

                // fastlinkURL = fastlinkURL + "?access_type=oauthdeeplink&displayMode=desktop&cbLocation=top&" + normalizerequestparameter + "&oauth_signature=" + strVal;
                fastlinkURL = fastlinkURL + "?" + normalizerequestparameter + "&oauth_signature=" + strVal;

                #region executeUserSearchRequest
                UserSearchRequest objUserSearchRequest = new UserSearchRequest();
                objUserSearchRequest.cobSessionToken = _Cobdata.cobrandConversationCredentials.sessionToken;
                objUserSearchRequest.userSessionToken = _Userdata.userContext.conversationCredentials.sessionToken;
                objUserSearchRequest.transactionSearchRequest = new TransactionSearchRequest { containerType = "insurance", higherFetchLimit = 500, lowerFetchLimit = 1 };
                objUserSearchRequest.transactionSearchRequest.resultRange = new ResultRange { endNumber = 100, startNumber = 1 };
                objUserSearchRequest.transactionSearchRequest.searchClients = new SearchClients { clientName = "DataSearchService", clientId = 1 };
                objUserSearchRequest.transactionSearchRequest.userInput = "rent";
                objUserSearchRequest.transactionSearchRequest.ignoreUserInput = true;
                objUserSearchRequest.transactionSearchRequest.searchFilter = new SearchFilter { transactionSplitType = "ALL_TRANSACTION" };

                string objUserSearchRequestURL = strYodleeDomain + "/services/srest/restserver/v1.0/jsonsdk/TransactionSearchService/executeUserSearchRequest";
                string postdata = JsonConvert.SerializeObject(objUserSearchRequest);
                string executeUserSearchRequest = string.Empty;
                executeUserSearchRequest = WebRequest(WebMethod.POST, objUserSearchRequestURL, postdata, null);

                #endregion

                #region GetTransactionCategoryTypes
                string strTransactionCategoryTypesData = GetTransactionCategoryTypes(_Cobdata.cobrandConversationCredentials.sessionToken, _Userdata.userContext.conversationCredentials.sessionToken);
                List<SearchSiteModel> lstSearchSiteModel = new List<SearchSiteModel>();
                lstSearchSiteModel = JsonConvert.DeserializeObject<List<SearchSiteModel>>(strTransactionCategoryTypesData);
                #endregion

                #region GetUserTransactionCategories
                string strUserTransactionCategoriesData = GetUserTransactionCategories(_Cobdata.cobrandConversationCredentials.sessionToken, _Userdata.userContext.conversationCredentials.sessionToken);
                List<UserTransactionCategoriesModel> lstUserTransactionCategoriesModel = new List<UserTransactionCategoriesModel>();
                lstUserTransactionCategoriesModel = JsonConvert.DeserializeObject<List<UserTransactionCategoriesModel>>(strUserTransactionCategoriesData);
                #endregion

                #region SearchSite
                string strSearchSiteData = SearchSite(_Cobdata.cobrandConversationCredentials.sessionToken, _Userdata.userContext.conversationCredentials.sessionToken, "america");
                List<TransactionCategoryTypesModel> lstTransactionCategoryTypesModel = new List<TransactionCategoryTypesModel>();
                lstTransactionCategoryTypesModel = JsonConvert.DeserializeObject<List<TransactionCategoryTypesModel>>(strSearchSiteData);
                #endregion

                #region strAccountSummaryAll
                string strAccountSummaryAll = GetAccountSummaryAll(_Cobdata.cobrandConversationCredentials.sessionToken, _Userdata.userContext.conversationCredentials.sessionToken);
                #endregion

                #region ExecuteUserSearchRequest
                ExecuteUserSearchRequestModel objExecuteUserSearchRequestModel = new ExecuteUserSearchRequestModel();
                string strExecuteUserSearchRequestData = ExecuteUserSearchRequest(_Cobdata.cobrandConversationCredentials.sessionToken, _Userdata.userContext.conversationCredentials.sessionToken);
                objExecuteUserSearchRequestModel = JsonConvert.DeserializeObject<ExecuteUserSearchRequestModel>(strExecuteUserSearchRequestData);
                #endregion

                #region GetUserTransactions
                string strUserTransactionsData = GetUserTransactions(_Cobdata.cobrandConversationCredentials.sessionToken, _Userdata.userContext.conversationCredentials.sessionToken, objExecuteUserSearchRequestModel.searchIdentifier.identifier);
                ExecuteUserSearchRequestSearchResultModel objExecuteUserSearchRequestSearchResultModel = new ExecuteUserSearchRequestSearchResultModel();
                objExecuteUserSearchRequestSearchResultModel = JsonConvert.DeserializeObject<ExecuteUserSearchRequestSearchResultModel>(strUserTransactionsData);
                #endregion
            }
            catch (Exception ex)
            {
                throw ex;
            }

            ViewBag.IframeURL = fastlinkURL;
            return View();
        }

        public static string WebRequest(WebMethod method, string url, string postData, string access_token, bool isAcceptAdded = false)
        {
            HttpWebRequest webRequest = null;
            StreamWriter requestWriter = null;
            string responseData = "";

            webRequest = System.Net.WebRequest.Create(url) as HttpWebRequest;
            webRequest.Method = method.ToString();
            webRequest.ServicePoint.Expect100Continue = false;
            webRequest.ContentType = "application/json";
            if (isAcceptAdded)
            {
                webRequest.Accept = "application/json";
            }
            //webRequest.UserAgent  = "Identify your application please.";
            //webRequest.Timeout = 20000;

            if (method == WebMethod.DELETE)
            {
                if (!string.IsNullOrEmpty(access_token))
                {
                    webRequest.Headers.Add("Authorization", "Bearer " + access_token);
                }
            }
            else if (method == WebMethod.POST)
            {
                if (!string.IsNullOrEmpty(access_token))
                {
                    webRequest.Headers.Add("Authorization", "Bearer " + access_token);
                }

                webRequest.ContentType = "application/json";
                webRequest.Accept = "application/json";
                //webRequest.ContentType = "application/x-www-form-urlencoded";

                //POST the data.
                requestWriter = new StreamWriter(webRequest.GetRequestStream());
                try
                {
                    requestWriter.Write(postData);
                }
                catch
                {
                    throw;
                }
                finally
                {
                    requestWriter.Close();
                    requestWriter = null;
                }
            }

            responseData = WebResponseGet(webRequest);
            webRequest = null;
            return responseData;
        }

        public static string WebRequestWithAccept(WebMethod method, string url, string postData)
        {
            HttpWebRequest webRequest = null;
            StreamWriter requestWriter = null;
            string responseData = "";

            webRequest = System.Net.WebRequest.Create(url) as HttpWebRequest;
            webRequest.Method = method.ToString();
            webRequest.ServicePoint.Expect100Continue = false;
            //webRequest.Headers.Add("Content-Type", "application/json");
            //webRequest.Headers.Add("Accept", "application/json");
            webRequest.ContentType = "application/json";
            webRequest.Accept = "application/json";

            /* requestWriter = new StreamWriter(webRequest.GetRequestStream());
             try
             {
                 requestWriter.Write(postData);
             }
             catch
             {
                 throw;
             }
             finally
             {
                 requestWriter.Close();
                 requestWriter = null;
             }*/

            responseData = WebResponseGet(webRequest);
            webRequest = null;
            return responseData;
        }

        public static string WebResponseGet(HttpWebRequest webRequest)
        {
            StreamReader responseReader = null;
            string responseData = "";

            try
            {
                responseReader = new StreamReader(webRequest.GetResponse().GetResponseStream());
                responseData = responseReader.ReadToEnd();
            }
            catch
            {
                throw;
            }
            finally
            {
                webRequest.GetResponse().GetResponseStream().Close();
                responseReader.Close();
                responseReader = null;
            }

            return responseData;
        }

        public string GetAccountSummaryAll(string cobSessionToken, string userSessionToken)
        {
            string straccountsummaryall = string.Empty;
            string summaryallURL = strYodleeDomain + "/services/srest/restserver/v1.0/account/summary/all";
            summaryallURL = summaryallURL + "?cobSessionToken=" + Server.UrlEncode(cobSessionToken) + "&userSessionToken=" + Server.UrlEncode(userSessionToken);
            straccountsummaryall = WebRequest(WebMethod.GET, summaryallURL, "", "", true);
            return straccountsummaryall;
        }

        public string ExecuteUserSearchRequest(string cobSessionToken, string userSessionToken)
        {
            string strUserSearchRequestResult = string.Empty;
            string userSearchRequestURL = strYodleeDomain + "/services/srest/restserver/v1.0/jsonsdk/TransactionSearchService/executeUserSearchRequest";
            userSearchRequestURL = userSearchRequestURL + "?cobSessionToken=" + Server.UrlEncode(cobSessionToken) + "&userSessionToken=" + Server.UrlEncode(userSessionToken);
            userSearchRequestURL = userSearchRequestURL + "&transactionSearchRequest.containerType=bank";
            userSearchRequestURL = userSearchRequestURL + "&transactionSearchRequest.higherFetchLimit=50000";
            userSearchRequestURL = userSearchRequestURL + "&transactionSearchRequest.lowerFetchLimit=1";
            userSearchRequestURL = userSearchRequestURL + "&transactionSearchRequest.resultRange.endNumber=100";
            userSearchRequestURL = userSearchRequestURL + "&transactionSearchRequest.resultRange.startNumber=1";
            userSearchRequestURL = userSearchRequestURL + "&transactionSearchRequest.searchClients.clientId=1";
            userSearchRequestURL = userSearchRequestURL + "&transactionSearchRequest.searchClients.clientName=DataSearchService";
            userSearchRequestURL = userSearchRequestURL + "&transactionSearchRequest.userInput=dag";
            userSearchRequestURL = userSearchRequestURL + "&transactionSearchRequest.searchFilter.transactionSplitType=ALL_TRANSACTION";
            strUserSearchRequestResult = WebRequest(WebMethod.POST, userSearchRequestURL, "", "", true);
            return strUserSearchRequestResult;

        }

        public string GetUserTransactions(string cobSessionToken, string userSessionToken, string identifier)
        {
            string strUserSearchRequestResult = string.Empty;
            string userSearchRequestURL = strYodleeDomain + "/services/srest/restserver/v1.0/jsonsdk/TransactionSearchService/getUserTransactions";
            userSearchRequestURL = userSearchRequestURL + "?cobSessionToken=" + Server.UrlEncode(cobSessionToken) + "&userSessionToken=" + Server.UrlEncode(userSessionToken);
            userSearchRequestURL = userSearchRequestURL + "&searchFetchRequest.searchIdentifier.identifier=" + identifier;
            userSearchRequestURL = userSearchRequestURL + "&searchFetchRequest.searchResultRange.startNumber=1";
            userSearchRequestURL = userSearchRequestURL + "&searchFetchRequest.searchResultRange.endNumber=500000";
            strUserSearchRequestResult = WebRequest(WebMethod.POST, userSearchRequestURL, "", "", true);
            return strUserSearchRequestResult;
        }

        public string SearchSite(string cobSessionToken, string userSessionToken, string siteSearchString)
        {
            string strSearchSiteResult = string.Empty;
            string SearchSiteURL = strYodleeDomain + "/services/srest/restserver/v1.0/jsonsdk/SiteTraversal/searchSite";
            SearchSiteURL = SearchSiteURL + "?cobSessionToken=" + Server.UrlEncode(cobSessionToken) + "&userSessionToken=" + Server.UrlEncode(userSessionToken);
            SearchSiteURL = SearchSiteURL + "&siteSearchString=" + siteSearchString;
            strSearchSiteResult = WebRequest(WebMethod.POST, SearchSiteURL, "", "", true);
            return strSearchSiteResult;
        }

        public string GetTransactionCategoryTypes(string cobSessionToken, string userSessionToken)
        {
            string strTransactionCategoryTypesResult = string.Empty;
            string strTransactionCategoryTypesURL = strYodleeDomain + "/services/srest/restserver/v1.0/jsonsdk/TransactionCategorizationService/getTransactionCategoryTypes";
            strTransactionCategoryTypesURL = strTransactionCategoryTypesURL + "?cobSessionToken=" + Server.UrlEncode(cobSessionToken) + "&userSessionToken=" + Server.UrlEncode(userSessionToken);
            strTransactionCategoryTypesResult = WebRequest(WebMethod.POST, strTransactionCategoryTypesURL, "", "", true);
            return strTransactionCategoryTypesResult;
        }

        public string GetUserTransactionCategories(string cobSessionToken, string userSessionToken)
        {
            string strUserTransactionCategoriesResult = string.Empty;
            string strUserTransactionCategoriesURL = strYodleeDomain + "/services/srest/restserver/v1.0/jsonsdk/TransactionCategorizationService/getUserTransactionCategories";
            strUserTransactionCategoriesURL = strUserTransactionCategoriesURL + "?cobSessionToken=" + Server.UrlEncode(cobSessionToken) + "&userSessionToken=" + Server.UrlEncode(userSessionToken);
            strUserTransactionCategoriesResult = WebRequest(WebMethod.POST, strUserTransactionCategoriesURL, "", "", true);
            return strUserTransactionCategoriesResult;
        }

        public string GetRefreshedUserItems(string cobSessionToken)
        {
            string strRefreshedUserItemsResult = string.Empty;
            string strRefreshedUserItemsURL = strYodleeDomain + "/services/srest/restserver/v1.0/jsonsdk/TransactionCategorizationService/getUserTransactionCategories";
            strRefreshedUserItemsURL = strRefreshedUserItemsURL + "?cobSessionToken=" + cobSessionToken;
            strRefreshedUserItemsURL = strRefreshedUserItemsURL + "&refreshDataFilter.requiredAll=true";
            strRefreshedUserItemsURL = strRefreshedUserItemsURL + "&refreshDataFilter.startDate=09-07-2015T10:05:01";
            strRefreshedUserItemsURL = strRefreshedUserItemsURL + "&refreshDataFilter.endDate=09-07-2015T10:06:01";
            strRefreshedUserItemsURL = strRefreshedUserItemsURL + "&refreshDataFilter.refreshType[0]=1";

            strRefreshedUserItemsResult = WebRequest(WebMethod.POST, strRefreshedUserItemsURL, "", "", true);
            return strRefreshedUserItemsResult;
        }
    }

    public enum WebMethod
    {
        GET,
        POST,
        DELETE,
        PUT,
        PATCH
    }

    public class userLoginAuthDetails
    {
        public userContext userContext { get; set; }
    }

    public class userContext
    {
        public cobrandConversationCredentials conversationCredentials { get; set; }
        public bool valid { get; set; }
        public string isPasswordExpired { get; set; }
        public string cobrandId { get; set; }
        public string channelId { get; set; }
        public string locale { get; set; }
        public string tncVersion { get; set; }
        public string applicationId { get; set; }
        public cobrandConversationCredentials cobrandConversationCredentials { get; set; }
    }

    public class cobLoginAuthDetails
    {
        public string cobrandId { get; set; }
        public string channelId { get; set; }
        public string locale { get; set; }
        public string tncVersion { get; set; }
        public string applicationId { get; set; }
        public cobrandConversationCredentials cobrandConversationCredentials { get; set; }
    }

    public class authTokens
    {
        public string token { get; set; }
        public string tokenSecret { get; set; }
        public string tokenLiveTime { get; set; }
        public string tokenCreationTime { get; set; }
        public bridgetKeyData bridgetKeyData { get; set; }
        public string memId { get; set; }
        public string longLifeToken { get; set; }
    }

    public class bridgetKeyData
    {
        public string bridgetAppId { get; set; }
    }

    public class cobrandConversationCredentials
    {
        public string sessionToken { get; set; }
    }

    public class UserSearchRequest
    {
        public string cobSessionToken { get; set; }
        public string userSessionToken { get; set; }
        public TransactionSearchRequest transactionSearchRequest { get; set; }
    }

    public class TransactionSearchRequest
    {
        public string containerType { get; set; }
        public int higherFetchLimit { get; set; }
        public int lowerFetchLimit { get; set; }
        public ResultRange resultRange { get; set; }
        public SearchClients searchClients { get; set; }
        public string userInput { get; set; }
        public bool ignoreUserInput { get; set; }
        public SearchFilter searchFilter { get; set; }
    }

    public class ResultRange
    {
        public int endNumber { get; set; }
        public int startNumber { get; set; }
    }

    public class SearchClients
    {
        public int clientId { get; set; }
        public string clientName { get; set; }
    }

    public class SearchFilter
    {
        public string transactionSplitType { get; set; }
    }
}