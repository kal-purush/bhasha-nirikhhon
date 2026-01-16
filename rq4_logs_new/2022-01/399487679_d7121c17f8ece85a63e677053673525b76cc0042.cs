using Microsoft.AspNetCore.Mvc;
using Igtampe.Neco.API.Requests;
using Igtampe.Neco.Common.Banking;
using Igtampe.Neco.Common.Income;
using Igtampe.Neco.Data;
using Igtampe.ChopoSessionManager;
using Microsoft.EntityFrameworkCore;
using Igtampe.Neco.Common.Taxes;
using QRCoder;
using SixLabors.ImageSharp;
using SixLabors.ImageSharp.PixelFormats;
using SixLabors.ImageSharp.Processing;
using SixLabors.ImageSharp.Drawing.Processing;
using SixLabors.Fonts;

namespace Igtampe.Neco.API.Controllers {

    /// <summary>Certification controller in charge of handling the generation of certification images, and the verification of said images and text</summary>
    [Route("API/Cert")]
    [ApiController]
    public class CertificationController:ControllerBase {

        private static readonly QRCodeGenerator qrGenerator = new();
        private static readonly Image NecoLogo = Image.Load(Properties.Resources.TinyNeco);
        private static readonly Image Checkmark = Image.Load(Properties.Resources.Check);
        
        private readonly FontCollection Fcollection = new();
        private readonly FontFamily Roboto;
        private readonly Font CertTextFont; //I cannot believe it is this difficult to get a font. Though, I wonder *why* it had to be done like this. Perhaps it provides some benefit I don't see

        private readonly NecoContext DB;

        /// <summary>Creates a User Controller</summary>
        /// <param name="Context"></param>
        public CertificationController(NecoContext Context) { 
            DB = Context;
            using MemoryStream MS = new(Properties.Resources.Roboto);
            Roboto = Fcollection.Install(MS); //google pls don't sue me
            CertTextFont = Roboto.CreateFont(16);

            //only now do I realize that there's a systemfonts object. Oh well, in the interest of cross platform-ness it's probably best to do this instead.

        }

        /// <summary>Generates a transaction certification receipt</summary>
        /// <param name="SessionID"></param>
        /// <param name="ID"></param>
        /// <returns></returns>
        [HttpGet("Transaction/{ID}")]
        public async Task<IActionResult> GenerateTransactionCert([FromHeader] Guid? SessionID, [FromRoute] Guid ID) {

            Session? S = await Task.Run(() => SessionManager.Manager.FindSession(SessionID ?? Guid.Empty));
            if (S is null) { return Unauthorized(ErrorResult.Reusable.InvalidSession); }

            Transaction? T = await DB.Transaction.Where(T => T.Origin != null && T.Destination != null)
#pragma warning disable CS8602 // Dereference of a possibly null reference.
                .Include(T => T.Origin).ThenInclude(D => D.Owners)
                .Include(T => T.Destination).ThenInclude(D => D.Owners)
#pragma warning restore CS8602 // Dereference of a possibly null reference.
                .FirstOrDefaultAsync(T => T.ID == ID);

            if (T is null || T.Origin is null || T.Destination is null) { return NotFound("Transaction was not found"); }

            if (!T.Origin.Owners.Any(A => A.ID == S.UserID) && !T.Destination.Owners.Any(A => A.ID == S.UserID)) {
                return Unauthorized("Transaction does not originate or destinate to account owned by session owner");
            }

            //ok we have all the data we need.

            TransactionQRCode TQR = new(T);

            //That's a lot of using for only generating *ONE* of these. at least we can re-use the QRCode generator (i hope)
            using QRCodeData qrCodeData = qrGenerator.CreateQrCode(TQR.ToString(), QRCodeGenerator.ECCLevel.Q);
            using PngByteQRCode qrCode = new(qrCodeData);
            using Image qrCodeImage = Image.Load(qrCode.GetGraphic(4,false));
            using Image CertImage = new Image<Rgba32>(500, 500);
            using Image SmolCheck = Checkmark.Clone(X => X.Resize(qrCodeImage.Size()));

            //Fill the background
            CertImage.Mutate(X => X.Fill(Color.White));

            //find the point where we should draw the neco logo at *d y n a m i c a l l y*
            int X = (CertImage.Width - NecoLogo.Width) / 2;
            Point NecoPoint = new(X, X); 

            //Draw it
            CertImage.Mutate(X => X.DrawImage(NecoLogo, NecoPoint, 1));

            //Find the point to draw the checkmark
            Point CheckmarkPoint = new(X+30, X + NecoLogo.Height + 20);

            //Draw it
            CertImage.Mutate(X => X.DrawImage(SmolCheck, CheckmarkPoint, 1));

            //Find the point to draw the QR Code
            Point QRPoint = new(CertImage.Width - X-30 - qrCodeImage.Width, X + NecoLogo.Height + 20);

            //Pray to the lord above that the QR Code and the Checkmark have even the slightest bit of breathing room
            //:Prayer emoji:

            //Draw it
            CertImage.Mutate(X => X.DrawImage(qrCodeImage, QRPoint, 1));

            //OK now text.
            //huh...

            //Find the point to draw the text
            Point TextPoint = new(CertImage.Width/2,X + NecoLogo.Height + 20 + SmolCheck.Height + 20); //I hope we can write text centered

            CertImage.Mutate(X => X.DrawText(
                new DrawingOptions() { TextOptions=new TextOptions() {HorizontalAlignment=HorizontalAlignment.Center } }, //gracias a dios we can
                TQR.ToCertImageString(),CertTextFont,Color.Black,TextPoint));

            return File(await ImageToPngByteArray(CertImage), "image/png");
        }

        /// <summary>Verifies a transaction qr code (more specifically the data in the qr code)</summary>
        /// <param name="TString"></param>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException"></exception>
        [HttpPost("Transaction")]
        public async Task<IActionResult> VerifyTransaction([FromBody] string TString) {

            TransactionQRCode TQR;

            try { TQR = new(TString); } catch (Exception E) {
                return Ok(new VerificationResult($"Error occured while parsing QR code data: {E.Message}"));
            }

            Transaction? T = await DB.Transaction.Include(T=>T.Origin).Include(T=>T.Destination).FirstOrDefaultAsync(T=>T.ID==TQR.ID);
            return T is null
                ? Ok(new VerificationResult($"Transaction with ID {TQR.ID} was not found"))
                : T.Origin is null || T.Destination is null
                    ? throw new InvalidOperationException("Somehow this transaction does not have an origin or a destination")
                    : T.Origin.ID != TQR.Origin
                        ? Ok(new VerificationResult($"Origin does not match"))
                        : T.Destination.ID != TQR.Destination
                            ? Ok(new VerificationResult($"Destination does not match"))
                            : T.Amount != TQR.Amount 
                                ? Ok(new VerificationResult($"Amount does not match")) 
                                : Ok(new VerificationResult());
        }

        private struct VerificationResult {
            
            public bool Pass { get; } = false;

            public string Reason { get; } = "";

            public VerificationResult() { Pass = true; Reason = ""; }
            public VerificationResult(string Reason) { Pass = false; this.Reason = Reason; }
        }

        private struct TransactionQRCode {
            public Guid ID { get; }
            public string Origin { get;  }
            public string Destination { get; }
            public long Amount { get; }

            public DateTime? Date { get; set; } = null;

            public TransactionQRCode(string T) {
                string[] TSplit = T.Split(',');
                ID = Guid.Parse(TSplit[0]);
                Origin= TSplit[1];
                Destination = TSplit[2];
                Amount = long.Parse(TSplit[3]);
            }

            public TransactionQRCode(Transaction T) {
                ID = T.ID;
                Origin = T.Origin?.ID ?? "";
                Destination = T.Destination?.ID ?? "";
                Amount = T.Amount;
                Date = T.Date;
            }

            public override string ToString() => string.Join(',', ID,Origin,Destination,Amount);

            public string ToCertImageString() 
                => $"{ID}\n" +
                   $"{Origin}--({Amount:n0}p)-->{Destination}\n" +
                   $"\n" +
                   $"{Date}";
        }

        private static async Task<byte[]> ImageToPngByteArray(Image I) {
            using MemoryStream ms = new();
            await I.SaveAsPngAsync(ms);
            return ms.ToArray();
        }
    }
}