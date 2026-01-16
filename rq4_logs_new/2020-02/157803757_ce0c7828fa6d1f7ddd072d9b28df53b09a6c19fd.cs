using SimpleSmtpInterceptor.Data.Models;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.RegularExpressions;

namespace SimpleSmtpInterceptor.Lib
{
    public class BoundaryParser
        : EmailParser
    {
        private readonly string _boundaryOpen;
        private readonly string _boundaryClose;
        private readonly Regex _attachmentName;

        public BoundaryParser(TextReader reader, bool verboseOutput)
            : base(reader, verboseOutput)
        {
            var line = Reader.ReadLine();

            var boundaryValue = line.Substring(Headers.Boundary.Length);

            _boundaryOpen = "--" + boundaryValue;

            _boundaryClose = _boundaryOpen + "--";

            _attachmentName = new Regex("Content-Type: .+; name=(.+)");
        }

        private void ParseMessage()
        {
            var line = GetNextLine();

            var sb = new StringBuilder();

            var boundaryCount = 0;

            while (line != null && boundaryCount < 2)
            {
                if (line == _boundaryOpen)
                {
                    boundaryCount++;

                    if (boundaryCount < 2)
                    {
                        line = GetNextLine();
                    }

                    continue;
                }

                if (line.StartsWith(Headers.ContentTransferEncoding))
                {
                    //Save to Header Info
                }
                else if (line.StartsWith(Headers.ContentType))
                {
                    //Save to Header Info
                }
                else
                {
                    sb.Append(line);
                }

                line = GetNextLine();
            }

            ParsedEmail.Email.Message = sb.ToString();
        }

        protected void ParseAttachments()
        {
            var line = GetNextLine();

            var lst = new List<EmailAttachment>();

            var endReached = false;

            while (!endReached)
            {
                var a = new EmailAttachment();

                var sb = new StringBuilder();

                while (line != null)
                {
                    if (line == _boundaryClose)
                    {
                        endReached = true;

                        break;
                    }

                    if (line == _boundaryOpen)
                    {
                        break;
                    }

                    if (line.StartsWith(Headers.ContentTransferEncoding))
                    {
                        //Save to Header Info
                    }
                    else if (line.StartsWith(Headers.ContentType))
                    {
                        //Save attachment name
                        var m = _attachmentName.Match(line);

                        a.Name = m.Success ? m.Groups[1].Value : Path.GetRandomFileName();
                    }
                    else if (line.StartsWith(Headers.ContentDisposition))
                    {
                        //Save to Header Info? Maybe?
                    }
                    else
                    {
                        sb.Append(line);
                    }

                    line = GetNextLine();
                }

                a.AttachmentBase64 = sb.ToString();

                lst.Add(a);

                line = GetNextLine();
            }

            ParsedEmail.Attachments = lst;
        }

        public override void ParseBody()
        {
            ParseMessage();

            ParseAttachments();
        }
    }
}