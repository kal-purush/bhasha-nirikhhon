using System;
using System.IO;
using System.Net;
using System.Text;
using Newtonsoft.Json;

namespace exercise1
{
    class Program
    {
        static void Main(string[] args)
        {
            var dataCenter = "us17";
            var apiKey = "ed37a130dbadee9671f725ebdf03583a-us17";
            var listId = "8f5f9903d4";
            var campaignId = "39494cd8ea";

            Console.WriteLine(GetLists(dataCenter, apiKey, ""));
            Console.WriteLine(CreateList(dataCenter, apiKey));
            Console.WriteLine(AddOrUpdateListMember(dataCenter, apiKey, listId));
            Console.WriteLine(CreateCampaign(dataCenter, apiKey, listId));
            Console.WriteLine(SetContent(dataCenter, apiKey, campaignId));
            Console.WriteLine(SendEmail(dataCenter, apiKey, campaignId));
            Console.ReadKey();
        }

        private static string GetLists(string dataCenter, string apiKey, string listId = "")
        {
            var uri = string.Format("https://{0}.api.mailchimp.com/3.0/lists/{1}", dataCenter, listId);
            try
            {
                using (var webClient = new WebClient())
                {
                    webClient.Headers.Add("Accept", "application/json");
                    webClient.Headers.Add("Authorization", "apikey " + apiKey);

                    return webClient.DownloadString(uri);
                }
            }
            catch (WebException we)
            {
                using (var sr = new StreamReader(we.Response.GetResponseStream()))
                {
                    return sr.ReadToEnd();
                }
            }
        }

        private static string CreateList(string dataCenter, string apiKey)
        {
            var sampleList = JsonConvert.SerializeObject(
                new
                {
                    name = "Xamarin test",
                    contact = new
                    {
                        company = "Novahub",
                        address1 = "155",
                        address2 = "Nguyen Van Linh",
                        city = "Danang",
                        state = "GA",
                        zip = "550000",
                        country = "VN",
                        phone = ""
                    },
                    permission_reminder = "Test reminder",
                    campaign_defaults = new
                    {
                        from_name = "Tran Ngoc Quoc",
                        from_email = "quoctran2124@gmail.com",
                        subject = "MailChimp Demo",
                        language = "en",
                    },
                    email_type_option = true
                });
            var uri = string.Format("https://{0}.api.mailchimp.com/3.0/lists", dataCenter);

            try
            {
                using (var webClient = new WebClient())
                {
                    webClient.Headers.Add("Accept", "application/json");
                    webClient.Headers.Add("Authorization", "apikey " + apiKey);

                    return webClient.UploadString(uri, "POST", sampleList);
                }
            }
            catch (WebException we)
            {
                using (var sr = new StreamReader(we.Response.GetResponseStream()))
                {
                    return sr.ReadToEnd();
                }
            }
        }

        private static string AddOrUpdateListMember(string dataCenter, string apiKey, string listId)
        {
            var sampleListMember = JsonConvert.SerializeObject(
                new
                {
                    email_address = "stevelee2421@outlook.com",
                    merge_fields =
                    new
                    {
                        FNAME = "Foo",
                        LNAME = "Bar"
                    },
                    status_if_new = "subscribed"
                });

            var hashedEmailAddress = string.IsNullOrEmpty(subscriberEmail) ? "" : CalculateMD5Hash(subscriberEmail.ToLower());
            var uri = string.Format("https://{0}.api.mailchimp.com/3.0/lists/{1}/members/{2}", dataCenter, listId, hashedEmailAddress);
            try
            {
                using (var webClient = new WebClient())
                {
                    webClient.Headers.Add("Accept", "application/json");
                    webClient.Headers.Add("Authorization", "apikey " + apiKey);
                    return webClient.UploadString(uri, "PUT", sampleListMember);
                }
            }
            catch (WebException we)
            {
                using (var sr = new StreamReader(we.Response.GetResponseStream()))
                {
                    return sr.ReadToEnd();
                }
            }
        }

        private static string CreateCampaign(string dataCenter, string apiKey, string listId)
        {
            var campaign = JsonConvert.SerializeObject(
                new
                {
                    recipients = new
                    {
                        list_id = listId
                    },
                    type = "regular",
                    settings = new
                    {
                        subject_line = "Test send email",
                        reply_to = "stevelee2421@outlook.com",
                        from_name = "Quoc Tran"
                    }
                });
            var uri = string.Format("https://{0}.api.mailchimp.com/3.0/campaigns", dataCenter);

            try
            {
                using (var webClient = new WebClient())
                {
                    webClient.Headers.Add("Accept", "application/json");
                    webClient.Headers.Add("Authorization", "apikey " + apiKey);

                    return webClient.UploadString(uri, "POST", campaign);
                }
            }
            catch (WebException we)
            {
                using (var sr = new StreamReader(we.Response.GetResponseStream()))
                {
                    return sr.ReadToEnd();
                }
            }
        }

        private static string SetContent(string dataCenter, string apiKey, string listId)
        {
            var content = JsonConvert.SerializeObject(
                new
                {
                    html = "<p>Hello there.</p>",

                });
            var uri = string.Format($"https://{dataCenter}.api.mailchimp.com/3.0/campaigns/{listId}/content");

            try
            {
                using (var webClient = new WebClient())
                {
                    webClient.Headers.Add("Accept", "application/json");
                    webClient.Headers.Add("Authorization", "apikey " + apiKey);

                    return webClient.UploadString(uri, "PUT", content);
                }
            }
            catch (WebException we)
            {
                using (var sr = new StreamReader(we.Response.GetResponseStream()))
                {
                    return sr.ReadToEnd();
                }
            }
        }

        private static string SendEmail(string dataCenter, string apiKey, string campaignId)
        {
            var content = JsonConvert.SerializeObject(
                new
                {
                    html = "<p>Hello there. </p>",

                });
            var uri = string.Format($"https://{dataCenter}.api.mailchimp.com/3.0/campaigns/{campaignId}/actions/send");

            try
            {
                using (var webClient = new WebClient())
                {
                    webClient.Headers.Add("Accept", "application/json");
                    webClient.Headers.Add("Authorization", "apikey " + apiKey);

                    return webClient.UploadString(uri, "POST", content);
                }
            }
            catch (WebException we)
            {
                using (var sr = new StreamReader(we.Response.GetResponseStream()))
                {
                    return sr.ReadToEnd();
                }
            }
        }

        private static string GetListMember(string dataCenter, string apiKey, string listId, string subscriberEmail = "")
        {
            var hashedEmailAddress = string.IsNullOrEmpty(subscriberEmail) ? "" : CalculateMD5Hash(subscriberEmail.ToLower());
            var uri = string.Format("https://{0}.api.mailchimp.com/3.0/lists/{1}/members/{2}", dataCenter, listId, hashedEmailAddress);
            try
            {
                using (var webClient = new WebClient())
                {
                    webClient.Headers.Add("Accept", "application/json");
                    webClient.Headers.Add("Authorization", "apikey " + apiKey);

                    return webClient.DownloadString(uri);
                }
            }
            catch (WebException we)
            {
                using (var sr = new StreamReader(we.Response.GetResponseStream()))
                {
                    return sr.ReadToEnd();
                }
            }
        }

        private static string CalculateMD5Hash(string input)
        {
            // Step 1, calculate MD5 hash from input.
            var md5 = System.Security.Cryptography.MD5.Create();
            byte[] inputBytes = System.Text.Encoding.ASCII.GetBytes(input);
            byte[] hash = md5.ComputeHash(inputBytes);

            // Step 2, convert byte array to hex string.
            var sb = new StringBuilder();
            foreach (var @byte in hash)
            {
                sb.Append(@byte.ToString("X2"));
            }
            return sb.ToString();
        }

    }
}