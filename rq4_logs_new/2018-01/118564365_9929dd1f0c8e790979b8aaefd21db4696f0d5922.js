logout = function () {
    if (confirm('Logout') == true) {
        window.location.href = 'scripts/do_logout.php';
    }
};
dialogPassword = function () {
    var content = '<div style="padding:10px;"><form id="formPassword" method="post">';
    content += '<table>';
    content += '<tr><td>Ketik Password Baru</td></tr>';
    content += '<tr><td><input id="new_password" name="new_password" type="password" class="easyui-textbox" style="height:30px;width:250px"></td></tr>';
    content += '<tr><td>Ulangi Lagi</td></tr>';
    content += '<tr><td><input id="new_password2" name="new_password2" type="password" class="easyui-textbox" style="height:30px;width:250px"></td></tr>';
    content += '</table>';
    content += '</form></div>';

    var dlg = $("<div></div>").dialog({
        width: 300, height: 200,
        content: content,
        title: 'Update Password',
        buttons: [
            {
                iconCls: 'icon-save',
                text: 'Update',
                handler: function () {
                    var pass1 = $("#new_password").textbox('getValue');
                    var pass2 = $("#new_password2").textbox('getValue');
                    console.log(pass1);
                    console.log(pass2);

                    if ((pass1 != pass2) || (pass1 == '' || pass2 == '')) {
                        alert('Password Tidak Sama');
                        return;
                    }
//                    $.ajax({
//                        url:'scripts/update_password.php',
//                        dataType
//                    });
                    $("#formPassword").form('submit', {
                        url: 'scripts/update_password.php',
                        success: function (data) {
                            data = eval('(' + data + ')');
                            $.messager.show({
                                title: data.code,
                                msg: data.msg
                            });
                            if (data.code == 'SUCCESS') {
                                dlg.dialog('close');
                            }
                        }
                    });
                }
            }, {
                iconCls: 'icon-cancel',
                text: 'Close',
                handler: function () {
                    dlg.dialog('close');
                }
            }
        ]
    }).dialog('open').dialog('center');
};
function Util() {
    var U = this;
    U.diff = function (date1, date2) {

    };
    U.formatdate = function (date) {
        var y = date.getFullYear();
        var m = date.getMonth() + 1;
        var d = date.getDate();
        return y + '-' + (m < 10 ? ('0' + m) : m) + '-' + (d < 10 ? ('0' + d) : d);
    };
    U.formatdatetime = function (date) {
        var y = date.getFullYear();
        var m = date.getMonth() + 1;
        var d = date.getDate();
        var hh = date.getHours();
        var mm = date.getMinutes();
        var ss = date.getSeconds();
        var sf = y + '-' + (m < 10 ? ('0' + m) : m) + '-' + (d < 10 ? ('0' + d) : d) + ' ' + (hh < 10 ? ('0' + hh) : hh) + ':' + (mm < 10 ? ('0' + mm) : mm) + ':' + (ss < 10 ? ('0' + ss) : ss);
        console.log(sf);
        return sf;
    };
    U.parsedate = function (s) {
        if (!s || s == undefined)
            return new Date();
        var ss = (s.split('-'));
        var y = parseInt(ss[0], 10);
        var m = parseInt(ss[1], 10);
        var d = parseInt(ss[2], 10);
        if (!isNaN(y) && !isNaN(m) && !isNaN(d)) {
            return new Date(y, m - 1, d);
        } else {
            return new Date();
        }
    };
    U.parsedatetime = function (s) {
        if (!s || s == undefined)
            return new Date();
        var dt = s.split(' ');
        var ss = (dt[0].split('-'));
        var y = parseInt(ss[0], 10);
        var m = parseInt(ss[1], 10);
        var d = parseInt(ss[2], 10);
        var ss2 = (dt[1].split(':'));
        var hh = parseInt(ss2[0], 10);
        var mm = parseInt(ss2[1], 10);
        var ss = parseInt(ss2[2], 10);
        if (!isNaN(y) && !isNaN(m) && !isNaN(d) && !isNaN(hh) && !isNaN(mm) && !isNaN(ss)) {
            return new Date(y, m - 1, d, hh, mm, ss);
        } else {
            return new Date();
        }
    };
    U.formatDownload = function (val, row) {
        return '<a href="#" onClick="javascript:alert("Download Disable");> Download File </a>';
    };
    U.formatStatus = function (value, row) {

        console.log(row);
        value = parseInt(row.progress, 10);
        var s = "";
        if (value < 50) {
            s = '<div style="width:100%;border:1px solid #ccc">' +
                    '<div style="width:' + value + '%;background:red;color:#fff">' + value + '%' + '</div>'
            '</div>';
        } else if (value <= 70) {
            s = '<div style="width:100%;border:1px solid #ccc">' +
                    '<div style="width:' + value + '%;background:orange;color:white">' + value + '%' + '</div>'
            '</div>';
        } else if (value <= 100) {
            s = '<div style="width:100%;border:1px solid #ccc">' +
                    '<div style="width:' + value + '%;background:green;color:white">' + value + '%' + '</div>'
            '</div>';
        }
        return s;
    };

}
var util = new Util();