using System.Collections.Generic;
using System.Configuration;
using Microsoft.Extensions.Configuration;

namespace Eds.Shared.Helper.VeribanGlobal.Library.FileManagement
{
    public class AccountEnvelopeFileService
    {
        private readonly FMFile _fmFile = null;
        private readonly string _AccountEnvelopeFileBase = new ConfigurationManager()["GLOBAL_AccountEnvelopeFileBase"];

        public AccountEnvelopeFileService()
        {
            _fmFile = new FMFile();
        }

        public KeyValuePair<bool, string> SaveDraftXml(string xmlContent, string envelopeNumber)
        {
            string destinationPath = _AccountEnvelopeFileBase + "Draft\\";

            return _fmFile.SaveXmlFile(xmlContent, destinationPath, envelopeNumber);
        }

        public KeyValuePair<bool, string> Move(string sourceFilePath, string accountParameter, bool ifExistOverwriteDestination)
        {
            string destinationPath = string.Format("{0}\\", accountParameter);
            destinationPath = _AccountEnvelopeFileBase + destinationPath;

            return _fmFile.MoveFile(sourceFilePath, destinationPath, ifExistOverwriteDestination);

        }
        public KeyValuePair<bool, string> MoveError(string sourceFilePath, string accountParameter, bool ifExistOverwriteDestination)
        {
            string destinationPath = string.Format("{0}\\Error\\", accountParameter);
            destinationPath = _AccountEnvelopeFileBase + destinationPath;

            return _fmFile.MoveFile(sourceFilePath, destinationPath, ifExistOverwriteDestination);
        }
    }
}