namespace Eds.Shared.Contracts;

public class GlobalEnums
{
    public enum RecordState
    {
        Active = 1,
        Passive = 2,
        Delete = 3
    }

    public enum VeribanSystemType
    {
        NONE = 0,
        MANAGER = 1,
        PORTAL = 2,
        EINVOICE = 11,
        EARCHIVE = 12,
        EBOOK = 13,
        ETICKET = 14,
        //FAKTORING = 15,
        EMUTABAKAT = 16,
        EDESPATCH = 17,
        EMANUFACTURE = 18,
        ESELFEMPLOYMENT = 19,
        EOKCDOCUMENT = 20,
        ERECEIPT = 21
    }

    public enum AccountActivateSystem
    {
        //AKTIVASYON SISTEMI                        OPTION_CODE         KAMU    OZEL    VUK507_KAMU VUK507_OZEL
        //Yeni kullanıcı ekleme                     usergb              1       2       3           4
        //Fatura saklama hizmeti                    archive             11      12      13          14
        //e -Arşiv hizmeti                          earchive            21      22      23          24
        //e -arşiv arşiv hizmeti                    archive_earchive    31      32      33          34
        //ebilet hizmeti                            eticket             41      42      43          44
        //İrsaliye hizmeti                          edespatch           51      52      53          54
        //İrsaliye arşiv hizmeti                    archive_edespatch   61      62      63          64
        //Serbest meslek makbuzu hizmeti            esevoucher          71      72      73          74
        //Serbest meslek makbuzu arşiv hizmeti      esevoucher_archive  91      92      93          94
        //Müsthasil makbuzu hizmeti                 epreceipt           81      82      83          84
        //Müsthasil makbuzu arşiv hizmeti           epreceipt_archive   101     102     103         104
        //Mali Rapor bildirim hizmeti(ESKİ CİHAZ)   erevenue            111     112     --          --
        //Mali Rapor bildirim hizmeti(YENİ CİHAZ)   erevenue            121     122     --          --
        //Dekont Hizmeti                            ebreceipt           131     132     --          --

        EINVOICE = 1,
        EINVOICE_STORAGE = 11,
        EARCHIVE = 21,
        EARCHIVE_STORAGE = 31,
        ETICKET = 41,
        EDESPATCH = 51,
        EDESPATCH_STORAGE = 61,
        ESELFEMPLOYMENT = 71,
        ESELFEMPLOYMENT_STORAGE = 91,
        EMANUFACTURED = 81,
        EMANUFACTURED_STORAGE = 101,
        OKCDOCUMENT_OLD_OKC = 111,
        OKCDOCUMENT_NEW_OKC = 121,
        ERECEIPT = 131
    }

    public enum VeribanPlatforms
    {
        NONE = 0,
        WEBSITE = 1,
        WEBSERVICE = 2,
        WINSERVICE = 3,
        VERIBANWINAPP = 4,
    }

    public enum EInvoicePlatforms
    {
        NONE = 0,
        WEBPORTAL = 1,
        WEBSERVICE_OldTransferWebService = 21,
        WEBSERVICE_AppConnectWebService = 22,
        WEBSERVICE_IntegrationWebService = 23,
        WEBSERVICE_LogoWebService = 24,
        WEBSERVICE_NetsisWebService = 25,
    }

    public enum EArchivePlatforms
    {
        NONE = 0,
        WEBPORTAL = 1,
        //WEBSERVICE_OldWebService = 21,
        WEBSERVICE_AppConnectWebService = 22,
        WEBSERVICE_IntegrationWebService = 23,
    }
    public enum EBookPlatforms
    {
        NONE = 0,
        WEBPORTAL = 1,
        WEBSERVICE_OldWebService = 21,
        WEBSERVICE_AppConnectWebService = 22,
        WEBSERVICE_IntegrationWebService = 23,
        WEBSERVICE_AppSignerWebService = 24,
    }
    public enum ETicketPlatforms
    {
        NONE = 0,
        WEBPORTAL = 1,
        WEBSERVICE_OldWebService = 21,
        WEBSERVICE_AppConnectWebService = 22,

        WEBSERVICE_HighWaysIntegrationWebService = 31,
        WEBSERVICE_SeaWaysIntegrationWebService = 32,
        WEBSERVICE_AirLinesIntegrationWebService = 33,
        WEBSERVICE_ActivityIntegrationWebService = 34,
    }
    public enum FaktoringPlatforms
    {
        NONE = 0,
        WEBPORTAL = 1,
        WEBSERVICE_BankService = 21,
    }

    public enum TransferDocumentDataTypes
    {
        XML_INZIP,
        TXT_INZIP,
        CSV_INZIP,
        XLS_INZIP,
    }

    public enum DownloadDocumentStates
    {
        UNKNOWN = 0,
        CREATING = 1,
        CREATED = 11,
        REMOVED = 12,
    }

    public enum DownloadOperationTypes
    {
        UNKNOWN = 0,

        EMAIL_REF_DOCUMENT = 1,
        COPY_REF_DOCUMENT = 2,
        PUBLIC_REF_DOCUMENT = 3,

        CREATE_HTML_FILE = 11,
        CREATE_IMAGE_FILE = 12,
        CREATE_PDF_FILE = 13,
        CREATE_MERGEPDF_FILE = 14,
        CREATE_ATTACHMENT_FILE = 15,

        REPORT_TEXT_FILE = 21,
        REPORT_CSV_FILE = 22,
        REPORT_EXCEL_FILE = 23,
    }

    public enum GlobalDocumentReferenceTypes
    {
        NO_REFERENCE = 0,
        MANAGER_TEST_DOCUMENT = 1,

        EINVOICE_SALES_INVOICE = 11,
        EINVOICE_SALES_INVOICE_ENVELOPE = 12,
        EINVOICE_SALES_INVOICE_ANSWER = 13,
        EINVOICE_SALES_INVOICE_ANSWER_ENVELOPE = 14,

        EINVOICE_SALES_INVOICE_TAXFREE_REJECTION = 29,

        EINVOICE_PURCHASE_INVOICE = 15,
        EINVOICE_PURCHASE_INVOICE_ENVELOPE = 16,
        EINVOICE_PURCHASE_INVOICE_ANSWER = 17,
        EINVOICE_PURCHASE_INVOICE_ANSWER_ENVELOPE = 18,

        EINVOICE_SALES_DESPATCHE = 21,
        EINVOICE_SALES_DESPATCHE_ENVELOPE = 22,
        EINVOICE_SALES_DESPATCHE_ANSWER = 23,
        EINVOICE_SALES_DESPATCHE_ANSWER_ENVELOPE = 24,

        EINVOICE_PURCHASE_DESPATCHE = 25,
        EINVOICE_PURCHASE_DESPATCHE_ENVELOPE = 26,
        EINVOICE_PURCHASE_DESPATCHE_ANSWER = 27,
        EINVOICE_PURCHASE_DESPATCHE_ANSWER_ENVELOPE = 28,

        EARCHIVE_SALES_INVOICE = 61,
        EARCHIVE_MANUFACTURED_RECEIPT = 62,
        EARCHIVE_SELF_EMPLOYMENT_RECEIPT = 63,
        EARCHIVE_OKC_OLD_DOCUMENT = 64,
        EARCHIVE_EXCEL_TRANSFER_QUEUE = 65, //EXCEL DONWLOAD REPORT
        EARCHIVE_EXCEL_SALES_INVOICE = 66, //EXCEL DONWLOAD REPORT
        EARCHIVE_EXCEL_MANUFACTURED_RECEIPT = 67, //EXCEL DONWLOAD REPORT
        EARCHIVE_EXCEL_SELF_EMPLOYMENT_RECEIPT = 68, //EXCEL DONWLOAD REPORT
        EARCHIVE_EXCEL_OKC_DOCUMENT = 69, //EXCEL DONWLOAD REPORT
        EARCHIVE_EXCEL_REPORT_PACKAGE = 161, //EXCEL DONWLOAD REPORT
        EARCHIVE_EXCEL_REPORT_PACKAGE_DETAIL = 162, //EXCEL DONWLOAD REPORT
        //EARCHIVE_SALES_INVOICE_ARCHIVE = 163,
        EARCHIVE_OKC_NEW_DOCUMENT = 164,
        EARCHIVE_OKC_INVOICE = 165,

        EBOOK_TRANSFER_DATA = 71,
        EBOOK_JOURNAL_BOOK = 72,
        EBOOK_JOURNAL_PATENT = 73,
        EBOOK_JOURNAL_GIB_PATENT = 74,
        EBOOK_LEDGER_BOOK = 75,
        EBOOK_LEDGER_PATENT = 76,
        EBOOK_LEDGER_GIB_PATENT = 77,
        EBOOK_EXCEL_TRANSFER_QUEUE = 78, //EXCEL DONWLOAD REPORT
        EBOOK_EXCEL_JOURNAL = 79, //EXCEL DONWLOAD REPORT
        EBOOK_EXCEL_LEDGER = 80, //EXCEL DONWLOAD REPORT

        ETICKET_HIGHWAYS_TICKET = 81,
        ETICKET_PASSENGERSHEET = 82,
        ETICKET_SEAWAYS_TICKET = 83,
        ETICKET_ACTIVITY_TICKET = 84,
        ETICKET_AIRLINES_TICKET = 85,
        ETICKET_EXCEL_TICKET = 86, //EXCEL DONWLOAD REPORT
        ETICKET_EXCEL_PASSENGERSHEET = 87, //EXCEL DONWLOAD REPORT
        ETICKET_EXCEL_TRANSFER_QUEUE = 88, //EXCEL DONWLOAD REPORT
        ETICKET_EXCEL_REPORT_PACKAGE = 89, //EXCEL DONWLOAD REPORT
        ETICKET_EXCEL_REPORT_PACKAGE_DETAIL = 90, //EXCEL DONWLOAD REPORT

        FAKTORING_INVOICE = 91,
        FAKTORING_EXCEL_INVOICE = 92, //EXCEL DONWLOAD REPORT
        FAKTORING_EXCEL_HEADER = 93, //EXCEL DONWLOAD REPORT

        EMUTABAKAT_SENDED_MUTABAKAT_CARI = 101,
        EMUTABAKAT_RECEIVED_MUTABAKAT_CARI = 102,
        EMUTABAKAT_SENDED_MUTABAKAT_BA_BS = 103,
        EMUTABAKAT_RECEIVED_MUTABAKAT_BA_BS = 104,
    }

    public enum UserProfilePermissionTypes
    {
        NONE = 0,

        CONTROLLER_ACTION_ACCESS = 1, //access controller action, CREATE,LIST etc.

        MENU_VIEW_ACCESS = 2, //Block permission, exm: Only received invoice operations

        RECORD_MANUPULATE_ACCESS = 3, //page list permission filter,spesific record modify OR remove permission
    }

    public enum ContactInfoTypes
    {
        NONE = 0,
        GENERAL = 1,
        MALI_MUSAVIR = 2,
        YEMINLI_MM = 3,
    }

    public enum GlobalUIStates
    {
        Default = 1,
        Wait = 2,
        Processing = 3,
        Error = 4,
        Success = 5,
    }

    public enum NewAccountEnvelopeStatus
    {
        None = 0,

        WaitForProcessUserAccount = 1,

        WaitForTransferTime = 3,
        TransferredToGib = 4,

        QueryError = 33,
        GIBDocumentError = 34,

        GIBDocumentCompleted = 64,
    }

    public enum NewAccountEnvelopeType
    {
        ProcessUserAccount = 1,
        CancelUserAccount = 2
    }

    public enum RegisterSectorType
    {
        None = 0,
        PublicSector = 1,
        PrivateSector = 2,
    }

    public enum RegisterNumberType
    {
        None = 0,
        Commercial = 1,
        Person = 2,
    }

    public enum RegisterAliasAccessType
    {
        NO_ALIAS = 0,
        GB_AND_PK = 1,
        Only_GB = 2,
        Only_PK = 3
    }

    public enum RegisterAliasDirection
    {
        Outbox = 1,
        Inbox = 2,
        Storage = 3,
        Process1 = 4,
        Process2 = 5,
        Process3 = 6,
        Process4 = 7
    }

    public enum EInvoiceAccountProcessType
    {
        None = 0,
        StandartAccount = 1,
        ArchiveEInvoice = 2,
    }
    public enum EArchiveAccountProcessType
    {
        None = 0,
        StandartAccount = 1,
    }
    public enum EBookAccountProcessType
    {
        None = 0,
        StandartAccount = 1,
        BookArchiver = 2,
    }
    public enum ETicketAccountProcessType
    {
        None = 0,
        Highways = 1,
        Seaways = 2,
        AirLines = 3,
        Activity = 4,
    }
    public enum FaktoringAccountProcessType
    {
        None = 0,
        StandartAccount = 1,
    }

    public enum DownloadDocumentDataTypes
    {
        XML_INZIP,
        HTML_INZIP,
        IMAGE_INZIP,
        PDF_INZIP,
    }

    public enum TransferServiceBase
    {
        WS_DraftFileTransfer = 1,
        WEB_Upload = 2
    }
    public enum SignatureStatus
    {
        None = 0,

        SignedByOwner = 1,

        SignedByPfx = 2,
    }

    public enum NewTransferQueueProcessState
    {
        DocumentAddedToQueue = 10,

        ProcessingTypeControl = 21,
        ProcessingDocument = 22,

        ErrorDocumentTypeControl = 41,
        ErrorDocumentProcess = 42,

        DebugTest = 61,
        DebugTest2 = 62,

        DocumentTypeControlled = 91,

        DocumentSuccessfullyProcessed = 99,
    }

    public enum NewFormProcessStatus
    {
        //GRAY
        Unknown = 0,
        State_WaitForApproved = 1,
        State_WaitForDocumentCreate = 11,
        //BLUE
        State_WaitForDocumentSign = 12,
        //YELLOW
        Process_DocumentCreating = 21,
        Process_DocumentSigning = 22,
        //RED
        Error_DocumentCreate = 31,
        Error_DocumentSign = 32,
        //GREEN
        State_SuccessDocumentSigned = 99,
    }

    public enum NewFormMailStatus
    {
        //GRAY
        Unknown = 0,
        State_NoEmailProcess = 1,
        State_WaitForRefDocument = 2,

        State_NoSmtpConfig = 5,

        //BLUE
        State_WaitForCreateMailOrder = 11,
        State_WaitForSend = 12,
        State_WaitForCallBack = 13,

        //YELLOW
        Processing_CreateMailOrder = 21,
        Processing_SendMail = 22,

        DebugTest = 61,

        //RED
        Error_CreateMailOrder = 31,
        Error_SendMail = 32,
        Error_CallBack = 33,

        //GREEN
        State_SuccessCallBack = 99,
    }

    public enum MutabakatAnswerState
    {
        Unknown = 0,
        WaitForAnswer = 1,
        Accepted = 2,
        Rejected = 3,
        Closed = 4,
        WaitForRouting = 5
    }

    public enum NewFormReportStatus
    {
        //GRAY
        Unknown = 0,
        State_NoReportOperation = 1,
        State_WaitForSignedDocument = 2,

        //BLUE
        State_WaitForReport = 11,

        //YELLOW
        Processing_PrepareReport = 21,

        //RED
        Error_ReportPrepare = 31,

        //GREEN
        State_SuccessReported = 99,
    }

    public enum NewFormFaktoringStatus
    {
        //GRAY
        Unknown = 0,
        State_NoFaktoringOperation = 1,
        State_WaitForSignedDocument = 2,
        State_WaitForReportedDocument = 3,
        State_WaitForApprovedFactoring = 5,
        //BLUE
        State_WaitForTransferToPool = 11,
        //YELLOW
        Process_TransferringToPool = 21,
        //RED
        Error_TransferToPool = 31,
        //GREEN
        State_SuccessTransferToPool = 99,
    }

    public enum FaktoringInvoiceStatus
    {
        Unknown = 0,
        State_Offerable = 1,
        State_Offered = 2,
    }

    public enum FaktoringHeaderStatus
    {
        Unknown = 0,
        State_WaitForResult = 21,
        Error_FaktoringFailed = 31,
        State_FaktoringSuccess = 99,
    }

    public enum FaktoringDetailStatus
    {
        //GRAY
        Unknown = 0,
        State_OfferCreated = 1,
        //BLUE
        State_OfferAccepted = 11,
        State_FaktoringApproved = 12,
        //YELLOW
        State_OfferRejected = 21,
        State_FaktoringCancelled = 22,
        //RED
        Error_FaktoringFailed = 31,
        //GREEN
        State_FaktoringSuccess = 99,
    }

    public enum GibReportPackageStatus
    {
        PreparedPackage = 1,
        CreatedDraftPackage = 2,
        Signed = 3,
        Transferred = 4,
        ReSend = 5,
        ReQuery = 6,

        CreatingDraft = 10,
        Signing = 11,
        Sending = 12,
        Querying = 13,
        ErrorAnalyzing = 14,

        CreateDraftError = 30,
        SignError = 31,
        SendError = 32,
        QueryError = 33,
        GIBDocumentError = 34,

        GIBDocumentCompleted = 64,

        DebugTest = 61,
    }

    public enum CreditTypes
    {
        UNKNOWN = 0,

        A_EINVOICE_EDESPATCH = 1,
        B_EARCHIVE_EMM = 2,
        C_ETICKET_ESMM = 3,
        D_EBOOK = 4,
        E_OTHER = 5,
    }

    public enum CreditProcessTypes
    {
        UNKNOWN = 0,

        ADD_CREDIT = 1,
        REMOVE_CREDIT = 2,

        EINVOICE_SALES = 11,
        EINVOICE_PURCHASE = 12,
        EDESPATCH_SALES = 13,
        EDESPATCH_PURCHASE = 14,

        EARCHIVE_SALES_INVOICE = 21,
        EMM_SALES = 22,

        ETICKET_DOCUMENT = 31,
        ESMM_SALES = 32,

        EBOOK_DOCUMENT = 41,

        EOKC_DOCUMENT = 51,
    }


    public enum EmailLinkType
    {
        None = 0,
        RefDownload = 1,
        GeneratePdf = 2,
    }

    public enum EmailOperationTypes
    {
        NONE = 0,

        EINVOICE_SALES_INVOICE_REPORT_SUCCESS = 11,
        EINVOICE_SALES_INVOICE_SINGLE_CUSTOMER = 12,
        EINVOICE_PURCHASE_INVOICE_REPORT_SUCCESS = 13,
        EINVOICE_SALES_INVOICE_REPORT_REJECTED = 14,
        //EINVOICE_SALES_INVOICE_REPORT_FAILED = 15,
        EINVOICE_PURCHASE_INVOICE_REPORT_REJECTED = 16,
        //EINVOICE_PURCHASE_INVOICE_REPORT_FAILED = 17,
        EINVOICE_SALES_INVOICE_SINGLE_ACCOUNT = 18,
        EINVOICE_PURCHASE_INVOICE_SINGLE_ACCOUNT = 19,

        EDESPATCH_SALES_DESPATCH_REPORT_SUCCESS = 21,
        EDESPATCH_SALES_DESPATCH_SINGLE_CUSTOMER = 22,
        EDESPATCH_PURCHASE_DESPATCH_REPORT_SUCCESS = 23,
        EDESPATCH_SALES_DESPATCH_REPORT_REJECTED = 24,
        //EDESPATCH_SALES_DESPATCH_REPORT_FAILED = 25,
        EDESPATCH_PURCHASE_DESPATCH_REPORT_REJECTED = 26,
        //EDESPATCH_PURCHASE_DESPATCH_REPORT_FAILED = 27,
        EDESPATCH_SALES_DESPATCH_SINGLE_ACCOUNT = 28,
        EDESPATCH_PURCHASE_DESPATCH_SINGLE_ACCOUNT = 29,

        EARCHIVE_SALES_INVOICE_REPORT_SUCCESS = 31,
        EARCHIVE_SALES_INVOICE_SINGLE_CUSTOMER = 32,
        EARCHIVE_OKC_INVOICE_REPORT_SUCCESS = 33,
        EARCHIVE_OKC_INVOICE_SINGLE_CUSTOMER = 34,

        //EMM_SALES_INVOICE_REPORT_SUCCESS = 41,
        //EMM_SALES_INVOICE_SINGLE_CUSTOMER = 42,

        //ESMM_SALES_INVOICE_REPORT_SUCCESS = 51,
        //ESMM_SALES_INVOICE_SINGLE_CUSTOMER = 52,
    }

    public enum HistoryDefaultTypes
    {
        Unknown = 0,
        Fault = 1,

        Entry = 11,
        Info = 12,
        Warning = 13,
        Error = 14,
        Success = 15,
    }
    public enum HistoryDocumentTypes
    {
        Unknown = 0,
        Fault = 1,

        EnvelopeEntry = 11,
        EnvelopeInfo = 12,
        EnvelopeWarning = 13,
        EnvelopeError = 14,
        EnvelopeSuccess = 15,

        AnswerEnvelopeEntry = 16,
        AnswerEnvelopeInfo = 17,
        AnswerEnvelopeWarning = 18,
        AnswerEnvelopeError = 19,
        AnswerEnvelopeSuccess = 20,

        InvoiceEntry = 21,
        InvoiceInfo = 22,
        InvoiceWarning = 23,
        InvoiceError = 24,
        InvoiceSuccess = 25,

        InvoiceAnswerEntry = 26,
        InvoiceAnswerInfo = 27,
        InvoiceAnswerWarning = 28,
        InvoiceAnswerError = 29,
        InvoiceAnswerSuccess = 30,

        InvoiceEnvelopeEntry = 31,
        InvoiceEnvelopeInfo = 32,
        InvoiceEnvelopeWarning = 33,
        InvoiceEnvelopeError = 34,
        InvoiceEnvelopeSuccess = 35,

        InvoiceAnswerEnvelopeEntry = 36,
        InvoiceAnswerEnvelopeInfo = 37,
        InvoiceAnswerEnvelopeWarning = 38,
        InvoiceAnswerEnvelopeError = 39,
        InvoiceAnswerEnvelopeSuccess = 40,

        DespatchEntry = 41,
        DespatchInfo = 42,
        DespatchWarning = 43,
        DespatchError = 44,
        DespatchSuccess = 45,

        DespatchAnswerEntry = 46,
        DespatchAnswerInfo = 47,
        DespatchAnswerWarning = 48,
        DespatchAnswerError = 49,
        DespatchAnswerSuccess = 50,

        DespatchEnvelopeEntry = 51,
        DespatchEnvelopeInfo = 52,
        DespatchEnvelopeWarning = 53,
        DespatchEnvelopeError = 54,
        DespatchEnvelopeSuccess = 55,

        DespatchAnswerEnvelopeEntry = 56,
        DespatchAnswerEnvelopeInfo = 57,
        DespatchAnswerEnvelopeWarning = 58,
        DespatchAnswerEnvelopeError = 59,
        DespatchAnswerEnvelopeSuccess = 60,

        AcoountCrud = 61,
        UserCrud = 62,
        UserDocument = 63,
        UserSeri = 64,
        CustomerCrud = 65,
        UserDocumentMailReport = 66,
    }

}

public class EInvoiceEnums
{
    public enum ReceivedGibQueueEnvelopeDocumentType
    {
        None = 0,

        PurchaseInvoiceEnvelope = 113,
        PurchaseDespatchEnvelope = 114,

        SalesInvoiceAnswerEnvelope = 133,
        SalesDespatchAnswerEnvelope = 134,

        SystemResponse = 151,
    }

    public enum SendGibQueueEnvelopeDocumentType
    {
        None = 0,

        SalesInvoiceEnvelope = 111,
        SalesDespatchEnvelope = 112,

        PurchaseInvoiceAnswerEnvelope = 131,
        PurchaseDespatchAnswerEnvelope = 132,

        EnvelopeAccountCreate = 141,
        EnvelopeAccountCancel = 142,

        SystemResponseReceivedGibQueueError = 152,
        SystemResponseReceivedEnvelopeSeller = 153,
        SystemResponseReceivedEnvelopeBuyerAnswer = 154,
    }

    public enum SendGibEnvelopeProcessState
    {
        //GRAY
        Unknown = 0,

        State_PreparedWaitForRefDocument = 10,//LOGO vs. direct transfer

        //BLUE
        State_PreparedWaitForCreateFile = 12,
        State_CreatedFileWaitForSendToGib = 14,
        State_TransferredWaitForQueryFromGib = 16,

        //YELLOW
        Processing_CreateFile = 22,
        Processing_SendToGib = 24,
        Processing_QueryFromGib = 26,

        //RED
        Error_CreateFile = 42,//32,//
        Error_SendToGib = 44,//34,//

        Fail_EnvelopeErrorOnGIB = 51,//41,//
        Fail_EnvelopeErrorOnReceiver = 52,//42,//

        //YELLOW
        Processing_DebugTest = 61,
        Processing_ErrorAnalyzing = 62,

        //GREEN
        State_EnvelopeSuccessfullySend = 99,
    }

    public enum ReceivedGibEnvelopeProcessState
    {
        //GRAY
        Unknown = 0,

        //BLUE
        State_TransferredWaitForQueryFromGib = 16,

        //YELLOW
        Processing_QueryFromGib = 26,

        //RED
        Fail_EnvelopeErrorOnGIB = 51,// 41,//
        Fail_EnvelopeErrorOnReceiver = 52,// 42,//

        //YELLOW
        Processing_DebugTest = 61,
        Processing_ErrorAnalyzing = 62,

        State_EnvelopeSuccessfullySend = 99,
    }

    public enum GibTransferStatus
    {
        /// <summary>
        /// Hic islem yapılmamıs. Secime hazır bekliyor
        /// </summary>
        None = 0,

        /// <summary>
        /// Transfer edilmek uzere set edilmek uzere bekliyor (kullanıcı tarafından onaylanmayı bekliyor)
        /// </summary>
        Wait = 1,

        /// <summary>
        /// Kullanıcı tarafından onaylandı
        /// </summary>
        Approved = 2,

        /// <summary>
        /// Kullanıcı tarafından gönderilmek üzere set edildi.
        /// </summary>
        Send = 4,

        /// <summary>
        /// Gib'e gönderiliyor.
        /// </summary>
        Sending = 8,

        /// <summary>
        /// Gib transferi basariyla gerceklesti.
        /// </summary>
        Transfered = 16,

        /// <summary>
        /// Gib transferi yapılamadı. Error
        /// </summary>
        Error = 32,

        /// <summary>
        /// Fatura Tamamlandı
        /// </summary>
        Completed = 64,
    }

    public enum NewGibDocumentProcessStatus
    {
        State_NoOperation = 0,
        State_WaitForDraftCreated = 1,
        State_DraftCreatedWaitForApproved = 2,

        //BLUE
        State_ApprovedWaitForDocumentRequest = 11,
        State_DocumentCreatedWaitForSign = 12,

        //YELLOW
        Process_DocumentSigning = 22,

        //RED
        Error_CancelDocumentByUser = 41,
        Error_DocumentSign = 42,

        //YELLOW
        Debug_Test = 61,

        //GREEN
        State_SuccessDocumentSigned = 99,
    }
    public enum NewGibDocumentEnvelopingStatus
    {
        //GRAY
        Unknown = 0,
        State_NoEnvelopeOperation = 1,
        State_WaitForSignedDocument = 2,

        State_PreparedWaitForRefDocument = 10,//LOGO vs. direct transfer

        State_WaitForPrepareEnvelope = 11,

        //BLUE
        State_PreparedWaitForCreateFile = 12,
        State_CreatedFileWaitForSendToGib = 14,
        State_TransferredWaitForQueryFromGib = 16,

        //YELLOW
        Processing_PrepareEnvelope = 21,

        //RED
        Error_PrepareEnvelope = 41,
        Error_CreateFile = 42,
        Error_SendToGib = 44,

        Fail_EnvelopeErrorOnGIB = 51,
        Fail_EnvelopeErrorOnReceiver = 52,

        //YELLOW
        Processing_DebugTest = 61,
        Processing_ErrorAnalyzing = 62,

        //GREEN
        State_EnvelopeSuccessfullySend = 99,
    }

    public enum TransferModuleTypes
    {
        Transfer_UblTr_Xml = 0,
        //Transfer_Veriban_Xml = 4,
        //Transfer_Mikro_Xml = 5,
        Transfer_SAP_Xml = 8
    }

    public enum SgkCompanyTypes
    {
        None = 0,
        SAGLIK_ECZ = 1,
        SAGLIK_HAS = 2,
        SAGLIK_OPT = 3,
        SAGLIK_MED = 4,
        ABONELIK = 5,
        MAL_HIZMET = 6,
        DIGER = 7
    }

    public enum BuyerAnswer
    {
        None = 0,

        //Wait = 1,

        Accepted = 2,

        Rejectted = 4,

        Restitute = 8
    }

    public enum DespatchAnswer
    {
        //Bilinmiyor
        None = 0,

        //Tam kabul
        CompletelyAccepted = 1,

        //Tam kabul (şikayet)
        CompletelyAcceptedWithComplaint = 2,

        //Kısmi kabul
        PartiallyAccepted = 3,

        //Tam ret
        CompletelyRejected = 4,

    }

    public enum GibInvoiceType
    {
        Seller = 1,
        Buyer = 2
    }

    public enum InstutePositionType
    {
        None = 0,
        /// <summary>
        /// Alıcı (account'un kestigi faturalardaki adresler icin)
        /// </summary>
        Buyyer = 1,

        /// <summary>
        /// Satıcı (account'un aldığı faturalardaki adresler icin)
        /// </summary>
        Seller = 2
    }

    public enum GeneralDocumentType
    {
        None = 0,
        SalesInvoice = 1,
        SalesDespatch = 2,
        SalesDespatchAnswer = 3,
        PurchaseInvoice = 4,
        PurchaseDespatch = 5,
        PurchaseDespatchAnswer = 6
    }

    public enum ProgressResultType
    {
        Success,

        Fail,

        Dublicate
    }
}

public class EArchiveEnums
{
    public enum InvoiceTransportationTypes
    {
        NONE = 0,
        ELEKTRONIK = 1,
        KAGIT = 2,
    }

    public enum SerieStatusType
    {
        None = 0,
        Active = 1,
        Passive = 2,
        Delete = 3
    }

    public enum SalesType
    {
        Normal = 1,
        Internet = 2
    }

    public enum TransferModuleTypes
    {
        Transfer_UblTr_Xml = 0,
        //Transfer_Veriban_Xml = 4,
        //Transfer_Mikro_Xml = 5,
        Transfer_SAP_Xml = 8,
        Transfer_OKC_INVOICE_Xml = 9,
        Transfer_MM_Xml = 10,
        Transfer_SMM_Xml = 11


    }
}

public class ETicketEnums
{
    public enum TicketDocumentType
    {
        SATIS,
        IADE,
    }

    public enum TicketPaymentType
    {
        BANKAKARTI,
        BEDELSIZ,
        KREDIKARTI,
        PUAN,
        MAHSUP,
        MAHSUPPUAN,
        NAKIT,
        PASS,
        PROMOSYON,
        ULASIMKARTI,
        DIGER,

        //AirLinePaymentTypes
        MIL,
        COKLU,
    }

    public enum TicketServiceType
    {
        SEYAHAT,
        BAGAJ,
        CEZA,
        IPTALDEGISIKLIKTAZMINATI,
        YEMEK,
        KOLTUKSECIMI,
        DIGER,
    }
}

public class EBookEnums
{
    public enum BookDocumentType
    {
        JOURNAL_BOOK = 1,
        LEDGER_BOOK = 2
    }

    public enum FiscalPeriodTypes
    {
        STANDART_FISCAL = 0,
        SPECIAL_FISCAL = 1,
        HALF_FISCAL = 2,
        CANCEL_PERIOD = 3,
    }
}

public class ContractEnums
{
    public enum ContractEmailProcessState
    {
        //GRAY
        Unknown = 0,
        State_WaitForRefDocument = 2,

        //BLUE
        State_WaitForSend = 12,

        //YELLOW
        Processing_SendMail = 22,

        DebugTest = 61,

        //RED
        Error_SendMail = 32,

        //GREEN
        State_SuccessCallBack = 99,
    }

    public enum ContractFormProcessStatus
    {
        //GRAY
        Unknown = 0,
        State_DraftFileCreatedWaitForApproveDraft = 1,          //taslak oluşturuldu, taslak onayı bekliyor.
        State_DraftApprovedWaitForMailSendCompleted = 2,        //taslak onaylandı, mail gönderimi bekliyor
        State_MailSendCompletedWaitForCustomerSendToVeriban = 3,//mail müşteriye ulaştı, müşterinin veribana xml i göndermesi bekleniyor.
        //BLUE
        State_CustomerSentFileWaitForPDFCreate = 11,            //müşteri xml i gönderdi, pdf oluşması bekleniyor.
        State_PDFCreatedWaitForPDFSign = 12,                    //pdf oluştu, imzalanması bekleniyor.
        //YELLOW
        Process_PDFCreating = 21,
        Process_PDFSigning = 22,
        //RED
        Error_PDFCreate = 31,
        Error_PDFSign = 32,
        //GREEN
        Success_PDFSigned = 99,
    }
}