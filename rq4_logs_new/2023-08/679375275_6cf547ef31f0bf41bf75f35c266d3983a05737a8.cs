//using MS.MODELS;
//using System.Text;
//using MS.MODELS.v2;
//using MS.REPO.ACTIONS;

// using MODELS;
using static ACTIONS.all.msactions;

namespace ACTIONS
{
    public interface Imsactions
    {
        string break_();
        T ConvertJsonObj<T>( string? jsonObj);
        NullObjectExceptionFindingWriter NullObjWriter {get;}
        /// <summary>
        /// Gets a Form File from a specified path v3
        /// </summary>
        /// <param name="oldfilePath"></param>
        /// <param name="root"></param>
        /// <returns></returns>
        Microsoft.AspNetCore.Http.IFormFile? GetImage3(string oldfilePath, string root);

        /// <summary>
        /// Gets a Form File from a specified path
        /// </summary>
        /// <param name="oldfilePath"></param>
        /// <param name="root"></param>
        /// <returns></returns>
        Microsoft.AspNetCore.Http.IFormFile? GetImage2(string oldfilePath, string root);

        /// <summary>
        /// Waits asyncronously
        /// </summary>
        /// <param name="num"></param>
        /// <returns></returns>
        Task<bool> WaitTask(int num);


        /// <summary>
        /// Decrypts a text coming from the client
        /// </summary>
        /// <param name="text"></param>
        /// <returns></returns>
        string DecryptFromClient(string text);

        /// <summary>
        /// Checks is an email is valid or not
        /// </summary>
        /// <param name="email"></param>
        /// <returns></returns>
        bool IsValidEmail(string email);

        /// <summary>
        /// Converts a text to a list of strings where there are spaces
        /// </summary>
        /// <param name="text"></param>
        /// <returns></returns>
        string[] GetKeyWords(string text);

        /// <summary>
        /// Encode a url string
        /// </summary>
        /// <param name="urlstring"></param>
        /// <returns></returns>
        string UrlEncode(string urlstring);

        /// <summary>
        /// Decode a url string
        /// </summary>
        /// <param name="urlstring"></param>
        /// <returns></returns>
        string UrlDecode(string urlstring);

        /// <summary>
        /// Convert integer to a Status Type Enum
        /// </summary>
        /// <param name="membership"></param>
        /// <param name="statuses"></param>
        /// <returns></returns>
        //object CONVERT_INT_TO_ENUM_STATUS(int membership, dynamic statuses);

        /// <summary>
        /// Gets the Controller Responses
        /// </summary>
        /// <returns></returns>
        all.msactions.ControllerResponses GetControllerResponses();

        /// <summary>
        /// Gets a ms Token Instance
        /// </summary>
        /// <returns></returns>
        all.msactions.Token GetToken();

        /// <summary>
        /// Encrypt by specified key
        /// </summary>
        /// <param name="emailOrusername"></param>
        /// <param name="whatIsBeingEncrypted"></param>
        /// <returns></returns>
        string Encrypt(string emailOrusername, string whatIsBeingEncrypted);

        /// <summary>
        /// Encrypt generically
        /// </summary>
        /// <param name="whatIsBeingEncrypted"></param>
        /// <returns></returns>
        string Encrypt(string whatIsBeingEncrypted);

        /// <summary>
        /// Decrypt by specified key
        /// </summary>
        /// <param name="emailOrusername"></param>
        /// <param name="whatIsBeingDecrypted"></param>
        /// <returns></returns>
        string Dencrypt(string emailOrusername, string whatIsBeingDecrypted);

        /// <summary>
        /// Decrypt generically
        /// </summary>
        /// <param name="whatIsBeingDecrypted"></param>
        /// <returns></returns>
        string Dencrypt(string whatIsBeingDecrypted);

        /// <summary>
        /// Encrypts the string
        /// </summary>
        /// <param name="plainText"></param>
        /// <returns></returns>
        string Base64Encode(string plainText);

        /// <summary>
        /// Decrypts the string
        /// </summary>
        /// <param name="base64EncodedData"></param>
        /// <returns></returns>
        string Base64Decode(string base64EncodedData);

        /// <summary>
        /// Encryption using Rsa Algorithm
        /// </summary>
        /// <param name="text"></param>
        /// <returns></returns>
        string v2_Encrypt(string text);

        string GetClientIpAddress(Microsoft.AspNetCore.Http.HttpContext httpContext);


        /// <summary>
        /// Decryption using Rsa Algorithm. returns default guid if text null
        /// </summary>
        /// <param name="text"></param>
        /// <returns></returns>
        string v2_Decrypt(string? text);


        ///// <summary>
        ///// Encryption using Rsa Algorithm v3
        ///// </summary>
        ///// <param name="text"></param>
        ///// <returns></returns>
        //string v3_Encrypt(string text);


        ///// <summary>
        ///// Decryption using Rsa Algorithm v3
        ///// </summary>
        ///// <param name="text"></param>
        ///// <returns></returns>
        //string v3_Decrypt(string text);

        /// <summary>
        /// Binary Search to Find business from a List by name
        /// </summary>
        /// <param name="businesses"></param>
        /// <param name="name"></param>
        /// <param name="timer"></param>
        /// <returns></returns>
        // List<MODELS.IBusiness> FindBusiness_in_ListOfObjects(List<MODELS.IBusiness> businesses, string name, System.Diagnostics.Stopwatch timer);

        /// <summary>
        /// Binary Search to Find business from a List by business number
        /// </summary>
        /// <param name="businesses"></param>
        /// <param name="id"></param>
        /// <param name="timer"></param>
        /// <returns></returns>
        // List<MODELS.IBusiness> FindBusiness_in_ListOfObjects(List<MODELS.IBusiness> businesses, int id, System.Diagnostics.Stopwatch timer);

        /// <summary>
        /// Sorts a list of Businesses
        /// </summary>
        /// <param name="bs"></param>
        /// <returns></returns>
        // List<MODELS.IBusiness> SortBusinesses(List<MODELS.IBusiness> bs);

        /// <summary>
        /// This converts a List of IBusiness to a List of Object[]
        /// </summary>
        /// <param name="l"></param>
        /// <returns></returns>
        // List<object[]> ConvertList_of_Type_to_object(List<IBusiness> l);


        /// <summary>
        /// Gets the image from the file path
        /// </summary>
        /// <param name="oldfilePath"></param>
        /// <returns></returns>
        Microsoft.AspNetCore.Http.IFormFile? GetImage(string oldfilePath, string root);

        /// <summary>
        /// This saves an image based from a specified folder name
        /// </summary>
        /// <param name="file"></param>
        /// <param name="FolderName"></param>
        /// <param name="rootPath"></param>
        /// <param name="encodedImageName"></param>
        /// <returns></returns>
        void SaveImage(Microsoft.AspNetCore.Http.IFormFile file, string FolderName, string rootPath, string encodedImageName);

        /// <summary>
        /// This saves the business image in Year , Month format
        /// </summary>
        /// <param name="file"></param>
        /// <param name="rootPath"></param>
        /// <param name="encodedImageName"></param>
        Task SaveBusinessImage(Microsoft.AspNetCore.Http.IFormFile file, string rootPath, string encodedImageName);

        /// <summary>
        /// This saves the Service image in Year , Month format
        /// </summary>
        /// <param name="file"></param>
        /// <param name="rootPath"></param>
        /// <param name="encodedImageName"></param>
        /// <returns></returns>
        Task SaveServiceImage(Microsoft.AspNetCore.Http.IFormFile file, string rootPath, string encodedImageName);

        /// <summary>
        /// This saves the Product image in Year , Month format
        /// </summary>
        /// <param name="file"></param>
        /// <param name="rootPath"></param>
        /// <param name="encodedImageName"></param>
        /// <returns></returns>
        Task SaveProductImage(Microsoft.AspNetCore.Http.IFormFile file, string rootPath, string encodedImageName);

        /// <summary>
        /// This converts the type and gets the objects emitted by its methods 
        /// </summary>
        /// <param name="t"></param>
        /// <returns></returns>
        ConvType<object> GetConvType<T>(Type t, object o);

        /// <summary>
        /// Converts a type and gets the name and method objects into a dictionary
        /// </summary>
        /// <param name="t"></param>
        /// <param name="o"></param>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="C"></typeparam>
        /// <returns></returns>
        ConvType<object,object> GetConvType<T,C>(Type t, object o);


    }
}
