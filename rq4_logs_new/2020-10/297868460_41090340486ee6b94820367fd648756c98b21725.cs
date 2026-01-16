using MailClient.lib.Models;
using System;
using System.Collections.Generic;
using System.Text;

namespace MailSender.Data.Stores.InDB
{
    class RecipientsStoreInDB : StoreInDB<Recipient>
    {
        public RecipientsStoreInDB(MailSenderDB db) : base(db) { }
    }
    class MessagesStoreInDB : StoreInDB<Message>
    {
        public MessagesStoreInDB(MailSenderDB db) : base(db) { }
    }
    class SenderStoreInDB : StoreInDB<Sender>
    {
        public SenderStoreInDB(MailSenderDB db) : base(db) { }
    }
}