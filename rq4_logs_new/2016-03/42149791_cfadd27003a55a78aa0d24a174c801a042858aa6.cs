using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;

namespace WHA_avac
{
    public class Mail163
    {
        Form1 form1;
        public string address;
        public string password;
        string QueeryTitle;
        string ContentRegex;
        string sid;
        string mid;
        string target;
        CookieCollection cookieContainer = null;
        int threadNo;

        bool haveLogin = false;

        public Mail163(int tn, string add, string pw, Form1 f, string queeryTitle, string contentRegex)
        {
            this.address = add;
            this.password = pw;
            this.form1 = f;
            this.QueeryTitle = queeryTitle;
            this.ContentRegex = contentRegex;
            this.threadNo = tn;
        }

        public Mail163(int tn, string add, string pw, Form1 f)
        {
            this.address = add;
            this.password = pw;
            this.form1 = f;
            this.threadNo = tn;
        }

        public int login()
        {
            string html = form1.weLoveYue(
                threadNo,
                form1,
                "https://mail.163.com/entry/cgi/ntesdoor?df=mail163_letter&from=web&funcid=loginone&iframe=1&language=-1&passtype=1&product=mail163&net=n&style=-1&race=378_264_390_gz&uid=" + address,
                //       https://mail.163.com/entry/cgi/ntesdoor?df=mail163_letter&from=web&funcid=loginone&iframe=1&language=-1&passtype=1&product=mail163&net=n&style=-1&race=378_264_390_gz&uid=15985830370@163.com
                "POST",
                "http://mail.163.com/",
                false,
                "savelogin=0&url2=http%3A%2F%2Fmail.163.com%2Ferrorpage%2Ferror163.htm&username=" + address + "&password=" + password,
                //savelogin=0&url2=http%3A%2F%2Fmail.163.com%2Ferrorpage%2Ferror163.htm&username=15985830370&password=?
                "mail.163.com",
                true
                );

            if (Regex.Match(html, @"<html><head><script type=""text/javascript"">window.location.href = ""http://mail.163.com/errorpage/error163").Success)
            {
                //            <html><head><script type="text/javascript">window.location.href = "http://mail.163.com/errorpage/error163
                form1.setLogT(threadNo, "email " + address + "password err!");
                return -2;
            }

            sid = Regex.Match(html, @"(?<=\.jsp\?sid=)\w+?(?=&df=)").Value;
            // .jsp?sid=JAmlshvoaqUOwBHuygoonwWhpMKSFGmL&df=mail163_letter"

            form1.setLogT(threadNo, "login " + address + " succeed.");


            //to get cookie: coremail.sid
            html = form1.weLoveYue(
                threadNo,
                form1,
                "http://hwwebmail.mail.163.com/js6/main.jsp?sid=" + sid + "&df=mail163_letter",
                //   http://hwwebmail.mail.163.com/js6/main.jsp?sid=JAmlshvoaqUOwBHuygoonwWhpMKSFGmL&df=mail163_letter
                "GET",
                "http://mail.163.com/",
                false,
                "",
                "hwwebmail.mail.163.com",
                true
                );
            /*
                        if (html.Contains("网易邮箱6.0版"))
                        {
                            form1.setLogT("login " + address + " succeed.");
                            haveLogin = true;
                            return 1;
                        }
                        else
                        {
                            form1.setLogT("login " + address + " failed.");
                            return -1;
                        }
                     */
            /*
            //to get cookie: JSESSIONID
            html = Form1.weLoveYue(
             form1,
             "http://hwwebmail.mail.163.com/contacts/call.do?uid=m" + address + "&sid=" + sid + "&from=webmail&cmd=newapi.getContacts&vcardver=3.0&ctype=all&attachinfos=yellowpage,frequentContacts&freContLim=20",
                //http://hwwebmail.mail.163.com/contacts/call.do?uid=m15985830370@163.com&sid=zBlWgvjqvNoMaBtMqNqqORPEhgODezIV&from=webmail&cmd=newapi.getContacts&vcardver=3.0&ctype=all&attachinfos=yellowpage,frequentContacts&freContLim=20
             "POST",
             "http://mail.163.com/",
             false,
             "order=[{\"field\":\"N\",\"desc\":\"false\"}]",
             "hwwebmail.mail.163.com"
             );
            */

            return 1;
        }

        public string queery(string queeryTitle, string contentRegex)
        {
            if (queeryTitle == null || queeryTitle == "")
            {
                queeryTitle = QueeryTitle;
            }
            if (contentRegex == null || contentRegex == "")
            {
                contentRegex = ContentRegex;
            }
            if (!haveLogin)
            {
                login();
            }

            while (true)
            {
                // get unread mails
                string html = form1.weLoveYue(
                    threadNo,
                    form1,
                    "http://hwwebmail.mail.163.com/js6/s?sid=" + sid + "&func=mbox:listMessages&FrameMasterMailPopupClose=1",
                    //       http://hwwebmail.mail.163.com/js6/s?sid=JAmlshvoaqUOwBHuygoonwWhpMKSFGmL&func=mbox:listMessages&FrameMasterMailPopupClose=1
                    "POST",
                    "http://mail.163.com/",
                    false,
                    "var=%3C%3Fxml%20version%3D%221.0%22%3F%3E%3Cobject%3E%3Cobject%20name%3D%22filter%22%3E%3Cobject%20name"
                    + "%3D%22flags%22%3E%3Cboolean%20name%3D%22read%22%3Efalse%3C%2Fboolean%3E%3C%2Fobject%3E%3C%2Fobject%3E"
                    + "%3Cstring%20name%3D%22order%22%3Edate%3C%2Fstring%3E%3Cboolean%20name%3D%22desc%22%3Etrue%3C%2Fboolean"
                    + "%3E%3Carray%20name%3D%22fids%22%3E%3Cint%3E1%3C%2Fint%3E%3Cint%3E3%3C%2Fint%3E%3Cint%3E18%3C%2Fint%3E"
                    + "%3C%2Farray%3E%3Cint%20name%3D%22limit%22%3E20%3C%2Fint%3E%3Cint%20name%3D%22start%22%3E0%3C%2Fint%3E"
                    + "%3Cboolean%20name%3D%22skipLockedFolders%22%3Etrue%3C%2Fboolean%3E%3Cboolean%20name%3D%22returnTag%22"
                    + "%3Etrue%3C%2Fboolean%3E%3Cboolean%20name%3D%22returnTotal%22%3Etrue%3C%2Fboolean%3E%3C%2Fobject%3E",

                    "hwwebmail.mail.163.com",
                true
                    );

                if (html.Contains("<string name=\"subject\">" + queeryTitle + "</string>"))
                {
                    mid = Regex.Match(html, @"(?<=<string name=""id"">).+?(?=<\/string>\n.*\n.*\n.*\n.*\n.*" + queeryTitle + ")").Value;
                    //<string name="id">47:1tbiLxuMOFUL4Qg6aQAAsk</string>
                    //......4 lines
                    //<string name="subject">ご注文予約案内</string>




                    /*
                    //POST requst the specific mail
                    html = Form1.weLoveYue(
                        form1,
                        "http://hwwebmail.mail.163.com/js6/s?sid="+sid+"&func=mbox:readMessage&deftabclick=t3&l=read&action=read",
               //        http://hwwebmail.mail.163.com/js6/s?sid=JASffhvoYoQOwBeuygooOMBOzURbdSPL&func=mbox:readMessage&deftabclick=t3&mbox_mobile_ul_icon_show=1&l=read&action=read
                        "POST",
                        "http://hwwebmail.mail.163.com/js6/main.jsp?sid=" + sid + "&df=mail163_letter",
                        false,
                        "var=%3C%3Fxml%20version%3D%221.0%22%3F%3E%3Cobject%3E%3Cstring%20name%3D%22id%22%3E"+ Form1.ToUrlEncode(mid) //171%3A1tbiqxSMOFUL7APXEQAAs8
                        + "%3C%2Fstring%3E%3Cboolean%20name%3D%22header%22%3Etrue%3C%2Fboolean%3E%3Cboolean%20name%3D%22returnImageInfo"
                        + "%22%3Etrue%3C%2Fboolean%3E%3Cboolean%20name%3D%22returnAntispamInfo%22%3Etrue%3C%2Fboolean%3E%3Cboolean"
                        + "%20name%3D%22autoName%22%3Etrue%3C%2Fboolean%3E%3Cobject%20name%3D%22returnHeaders%22%3E%3Cstring%20name"
                        + "%3D%22Resent-From%22%3EA%3C%2Fstring%3E%3Cstring%20name%3D%22Sender%22%3EA%3C%2Fstring%3E%3Cstring%20name"
                        + "%3D%22List-Unsubscribe%22%3EA%3C%2Fstring%3E%3Cstring%20name%3D%22Reply-To%22%3EA%3C%2Fstring%3E%3C%2Fobject"
                        + "%3E%3Cboolean%20name%3D%22supportTNEF%22%3Etrue%3C%2Fboolean%3E%3C%2Fobject%3E",

                        "mail.163.com"
                        );
                    */


                    //GET the content
                    html = form1.weLoveYue(
                        threadNo,
                        form1,
                        "http://hwwebmail.mail.163.com/js6/read/readhtml.jsp?mid=" + mid + "&font=15&color=064977", //171:1tbiqxSMOFUL7APXEQAAs8
                            //http://hwwebmail.mail.163.com/js6/read/readhtml.jsp?mid=171:1tbiqxSMOFUL7APXEQAAs8&font=15&color=064977
                        "GET",
                        "http://hwwebmail.mail.163.com/js6/main.jsp?sid=" + sid + "&df=mail163_letter",
                        false,
                        "",
                        "hwwebmail.mail.163.com",
                        true
                    );


                    target = Regex.Match(html, contentRegex).Value;

                    //      <pre>下記URLより予約フォームにお進みいただき、ご登録いただくと予約が完了いたします。

                    //      https://aksale.advs.jp/cp/akachan_sale_pc/reg?id=w9EI8lKSAvC4he7hZIEWESR6JYXLHg1w


                    //(c)AKACHAN HONPO

                    form1.setLogtRed(threadNo, "got the url");
                    return target;
                }
                else
                {
                    form1.setLogtRed(threadNo, "do not find the notification mail, retry in 100ms");
                    Thread.Sleep(100);
                }
            }

        }


        public string queeryReaded(string queeryTitle, string contentRegex)
        {
            if (queeryTitle == null || queeryTitle == "")
            {
                queeryTitle = QueeryTitle;
            }
            if (contentRegex == null || contentRegex == "")
            {
                contentRegex = ContentRegex;
            }
            if (!haveLogin)
            {
                login();
            }

            while (true)
            {
                // get readed mails
                string html = form1.weLoveYue(
                    threadNo,
                    form1,
                    "http://hwwebmail.mail.163.com/js6/s?sid=" + sid + "&func=mbox:listMessages&TopTabReaderShow=1&TopTabLofterShow=1&welcome_welcomemodule_mailrecom_click=1&LeftNavfolder1Click=1&mbox_folder_enter=1",
                    //http://hwwebmail.mail.163.com/js6/s?sid=wAmXlMjmMrQocSziZrmmrOINFjUUvWpC&func=mbox:listMessages&TopTabReaderShow=1&TopTabLofterShow=1&welcome_welcomemodule_mailrecom_click=1&LeftNavfolder1Click=1&mbox_folder_enter=1
                    "POST",
                    "http://mail.163.com/",
                    false,
                    "var=%3C%3Fxml%20version%3D%221.0%22%3F%3E%3Cobject%3E%3Cint%20name%3D%22fid%22%3E1%3C%2Fint%3E%3Cstring"
+ "%20name%3D%22order%22%3Edate%3C%2Fstring%3E%3Cboolean%20name%3D%22desc%22%3Etrue%3C%2Fboolean%3E%3Cint"
+ "%20name%3D%22limit%22%3E20%3C%2Fint%3E%3Cint%20name%3D%22start%22%3E0%3C%2Fint%3E%3Cboolean%20name%3D"
+ "%22skipLockedFolders%22%3Efalse%3C%2Fboolean%3E%3Cstring%20name%3D%22topFlag%22%3Etop%3C%2Fstring%3E"
+ "%3Cboolean%20name%3D%22returnTag%22%3Etrue%3C%2Fboolean%3E%3Cboolean%20name%3D%22returnTotal%22%3Etrue"
+ "%3C%2Fboolean%3E%3C%2Fobject%3E",


                    "hwwebmail.mail.163.com",
                true
                    );

                //    if (html.Contains("<string name=\"subject\">"+queeryTitle+"</string>"))
                if (true)
                {
                    mid = Regex.Match(html, @"(?<=<string name=""id"">).+?(?=<\/string>\n.*\n.*\n.*\n.*\n.*" + queeryTitle + ")").Value;


                    /*
                    //POST requst the specific mail
                    html = Form1.weLoveYue(
                        form1,
                        "http://hwwebmail.mail.163.com/js6/s?sid="+sid+"&func=mbox:readMessage&deftabclick=t3&l=read&action=read",
               //        http://hwwebmail.mail.163.com/js6/s?sid=JASffhvoYoQOwBeuygooOMBOzURbdSPL&func=mbox:readMessage&deftabclick=t3&mbox_mobile_ul_icon_show=1&l=read&action=read
                        "POST",
                        "http://hwwebmail.mail.163.com/js6/main.jsp?sid=" + sid + "&df=mail163_letter",
                        false,
                        "var=%3C%3Fxml%20version%3D%221.0%22%3F%3E%3Cobject%3E%3Cstring%20name%3D%22id%22%3E"+ Form1.ToUrlEncode(mid) //171%3A1tbiqxSMOFUL7APXEQAAs8
                        + "%3C%2Fstring%3E%3Cboolean%20name%3D%22header%22%3Etrue%3C%2Fboolean%3E%3Cboolean%20name%3D%22returnImageInfo"
                        + "%22%3Etrue%3C%2Fboolean%3E%3Cboolean%20name%3D%22returnAntispamInfo%22%3Etrue%3C%2Fboolean%3E%3Cboolean"
                        + "%20name%3D%22autoName%22%3Etrue%3C%2Fboolean%3E%3Cobject%20name%3D%22returnHeaders%22%3E%3Cstring%20name"
                        + "%3D%22Resent-From%22%3EA%3C%2Fstring%3E%3Cstring%20name%3D%22Sender%22%3EA%3C%2Fstring%3E%3Cstring%20name"
                        + "%3D%22List-Unsubscribe%22%3EA%3C%2Fstring%3E%3Cstring%20name%3D%22Reply-To%22%3EA%3C%2Fstring%3E%3C%2Fobject"
                        + "%3E%3Cboolean%20name%3D%22supportTNEF%22%3Etrue%3C%2Fboolean%3E%3C%2Fobject%3E",

                        "mail.163.com"
                        );
                    */


                    //GET the content
                    html = form1.weLoveYue(
                        threadNo,
                        form1,
                        "http://hwwebmail.mail.163.com/js6/read/readhtml.jsp?mid=" + mid + "&font=15&color=064977", //171:1tbiqxSMOFUL7APXEQAAs8
                            //http://hwwebmail.mail.163.com/js6/read/readhtml.jsp?mid=171:1tbiqxSMOFUL7APXEQAAs8&font=15&color=064977
                        "GET",
                        "http://hwwebmail.mail.163.com/js6/main.jsp?sid=" + sid + "&df=mail163_letter",
                        false,
                        "",
                        "hwwebmail.mail.163.com",
                true
                    );


                    target = Regex.Match(html, contentRegex).Value;

                    //      <pre>下記URLより予約フォームにお進みいただき、ご登録いただくと予約が完了いたします。

                    //      https://aksale.advs.jp/cp/akachan_sale_pc/reg?id=w9EI8lKSAvC4he7hZIEWESR6JYXLHg1w


                    //(c)AKACHAN HONPO

                    form1.setLogtRed(threadNo, "got the url");
                    return target;
                }
                else
                {
                    form1.setLogtRed(threadNo, "do not find the notification mail");
                }
            }

        }

    }

}