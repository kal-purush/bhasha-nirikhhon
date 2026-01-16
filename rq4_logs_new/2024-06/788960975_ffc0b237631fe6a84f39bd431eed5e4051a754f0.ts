import { getFormattedDate, todayDate } from '../utility/date.utility';

export async function cancelledTransaction(data) {
  const content = `<!DOCTYPE html>
<html lang="en-US">
  <head>
    <meta content="text/html; charset=utf-8" http-equiv="Content-Type" />
    <title>Cancelled Transaction</title>
    <meta name="description" content="New Account Email Template." />
    <style type="text/css">
      a:hover {
        text-decoration: underline !important;
      }
    </style>
  </head>

  <body marginheight="0" topmargin="0" marginwidth="0" leftmargin="0" style="margin:0;background-color:#f2f3f8;font-family:'Open Sans',sans-serif;">
    <!--[if !gte mso 9]><!-->
    <span style="display:none;font-size:0px;line-height:0px;max-height:0px;max-width:0px;opacity:0;overflow:hidden;visibility:hidden;mso-hide:all;">Welcome to Support Our Heroes. Donation Transaction Cancelled.&nbsp;&#847;&nbsp;&#847;&nbsp;&#847;&nbsp;&#847;&nbsp;&#847;&nbsp;&#847;&nbsp;&#847;&nbsp;&#847;&nbsp;&#847;&nbsp;&#847;&nbsp;&#847;&nbsp;&#847;&nbsp;&#847;&nbsp;&#847;&nbsp;&#847;&nbsp;&#847;&nbsp;&#847;&nbsp;&#847;&nbsp;&#847;&nbsp;&#847;&nbsp;&#847;&nbsp;&#847;&nbsp;&#847;&nbsp;&#847;</span>
    <!--<![endif]-->
    <table cellspacing="0" border="0" cellpadding="0" width="100%" bgcolor="#f2f3f8">
      <tr>
        <td>
          <table width="100%" border="0" align="center" cellpadding="0" cellspacing="0" style="background-color:#f2f3f8;max-width:670px;margin:0 auto;">
            <tr>
              <td style="height:80px"> </td>
            </tr>
            <tr>
              <td style="text-align:center">
                <a href="https://supportourheroes.in/" title="logo" target="_blank">
                  <img width="150" src="https://supportourheroes.in/wp-content/uploads/2018/04/cropped-A2-3-300x295.png" title="logo" alt="logo" />
                </a>
              </td>
            </tr>
            <tr>
              <td style="height:20px"> </td>
            </tr>
            <tr>
              <td>
                <table width="95%" border="0" align="center" cellpadding="0" cellspacing="0" style="max-width:670px;background:#fff;border-radius:3px;text-align:center;box-shadow:0 6px 18px 0 rgba(0,0,0,0.06);">
                  <tr>
                    <td style="height:40px"> </td>
                  </tr>
                  <tr>
                    <td style="padding:0 35px">
                      <h1 style="color:#1e1e2d;font-weight:500;margin:0;font-size:32px;font-family:'Rubik',sans-serif;">Donation Status Update</h1>
                      <span style="display:inline-block;vertical-align:middle;margin:29px 0 26px;border-bottom:1px solid #cecece;width:100px;"></span>
                      <p style="font-size:18px;color:#455056;line-height:20px;margin:0;font-weight:500;">
                        <strong style="display:block;font-size:13px;margin:0 0 4px;color:rgba(0,0,0,0.64);font-weight:normal;">Amount</strong>${data?.amount}
                        <strong style="display:block;font-size:13px;margin:24px 0 4px 0;font-weight:normal;color:rgba(0,0,0,0.64);">Donor Name</strong>${data?.donor_first_name}
                        <strong style="display:block;font-size:13px;margin:24px 0 4px 0;font-weight:normal;color:rgba(0,0,0,0.64);">Email</strong>${data?.donor_email}
                        <strong style="display:block;font-size:13px;margin:24px 0 4px 0;font-weight:normal;color:rgba(0,0,0,0.64);">Mobile Number</strong>${data?.donor_phone}
                        <strong style="display:block;font-size:13px;margin:24px 0 4px 0;font-weight:normal;color:rgba(0,0,0,0.64);">PAN</strong>${data?.pan}
                        <strong style="display:block;font-size:13px;margin:24px 0 4px 0;font-weight:normal;color:rgba(0,0,0,0.64);">Address</strong>${data?.donor_address}
                        <br>
                        <br>
                        <strong style="display:block;font-size:13px;margin:24px 0 4px 0;font-weight:normal;color:rgba(0,0,0,0.64);">Reference</strong>${data?.reference_payment}
                        <strong style="display:block;font-size:13px;margin:24px 0 4px 0;font-weight:normal;color:rgba(0,0,0,0.64);">Donation ID</strong>${data?.donation_id_frontend}
                        <strong style="display:block;font-size:13px;margin:24px 0 4px 0;font-weight:normal;color:rgba(0,0,0,0.64);">80G Certificate</strong>${data?.certificate}
                        <strong style="display:block;font-size:13px;margin:24px 0 4px 0;font-weight:normal;color:rgba(0,0,0,0.64);">Date & Time</strong>${await getFormattedDate(data?.created_at)}
                      </p>
                    </td>
                  </tr>
                  <tr>
                    <td style="height:40px"> </td>
                  </tr>
                </table>
              </td>
            </tr>
            <tr>
              <td style="height:20px"> </td>
            </tr>
            <tr>
              <td style="text-align:center">
                <p style="font-size:14px;color:rgba(69,80,86,0.7411764705882353);line-height:18px;margin:0 0 0;">© <strong>${await todayDate()} Support Our Heroes</strong></p>
              </td>
            </tr>
            <tr>
              <td style="height:80px"> </td>
            </tr>
          </table>
        </td>
      </tr>
    </table>
  </body>
</html>`;
  return content;
}