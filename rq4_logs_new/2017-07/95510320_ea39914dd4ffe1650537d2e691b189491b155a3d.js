getCompanylist();

function getCompanylist(){
    $.ajax({
        url:'a/clist',
        type:'GET',
        data:{
            page:1,/*你们设置变量改数就好*/
            rows:14,
        },
        success:function(data){
            if(data.code==1){
                var list=data.data;
                var obj=JSON.parse(list);
                var arr_data=obj.data;/*data 的数组*/

                //console.log(arr_data+"~~~~~~~");
                var total=obj.total;/* 总量*/
                for(var i in arr_data){
                    //console.log("~"+arr_data[i].id+"~"+arr_data[i].name+"~"+arr_data[i].manager_name);
                    $("#companylist").append("<tr><td>"+i+"</td><td id='name'>"+arr_data[i].name+"</td><td id='m_name'>"+arr_data[i].manager_name+"</td><td id='m_id'>"+arr_data[i].m_id+"</td><td></td></tr>")
                    $("#allapplist").append("<tr><td>"+i+"</td><td hidden id='company_id'>"+arr_data[i].id+"</td><td id='name'>"+arr_data[i].name+"</td><td id='m_name'>"+arr_data[i].manager_name+"</td><td id='m_id'>"+arr_data[i].m_id+"</td><td><button>管理</button></td></tr>")

                }
                $(".biao1 button").on('click',function(){
                    $(".xmguanli").attr("style","display:none;");
                    $(".glyshezhi").attr("style","display:block;");
                    $(".fenye").attr("style","display:block;");
                    var self = $(this);
                    var company_id = self.parents("tr").find("#company_id").text();
                    var company_name= self.parents("tr").find("#name").text();
                    console.log(company_id);
                    $("#gs_name").text(company_name);
                    $("#company_applist tr").remove();
                    $("#company_applist").append("<tr><th>序号</th><th>项目名</th> <th>项目ID</th> <th>开始时间</th> <th>结束时间</th> <th>备注</th> <th></th> </tr>");
                    getApplist(company_id);

                });
                $(".biao1 button").mouseover(function(){
                    $(this).css("color","black");
                });
                $(".biao1 button").mouseout(function(){
                    $(this).css("color","#fff");
                });

            }else console.log(data.status);
        },
    })
}

function getApplist(company_id){
    $.ajax({
        url:'a/alist/company/'+company_id,
        type:'GET',
        data:{
            page:1,
            rows:14,
        },
        success:function(data){
            if(data.code==1){
                var list=data.data;
                var obj=JSON.parse(list);
                var arr_data=obj.data;/*data 的数组*/

                console.log(arr_data+"~~~~~~~");
                var total=obj.total;/* 总量*/
                for(var i in arr_data){
                    console.log(arr_data[i].id+"----"+arr_data[i].name+"----"+arr_data[i].description);
             $('#company_applist tbody').append("<tr> <td>"+ i+"</td> <td class='xiangmu_name' id='xiangmu_name"+arr_data[i].name+"'>"+arr_data[i].name+"</td> <td id='xiangmu_id"+arr_data[i].id+"'  class='xiangmu_id'>"+arr_data[i].id+"</td> <td class='start_time' id='start_time"+arr_data[i].starttime+"'>"+arr_data[i].starttime+"</td> <td>"+arr_data[i].endtime+"</td> <td class='descrip-tion'>"+arr_data[i].description+"</td> <td> "+
                 "<button class='chakan'>查看</button> <button class='btn g2' data-toggle='modal' data-target='#myModa2'><p>修改</p> </button> <button class='app-delete'>删除</button> </td> </tr>")
                }

                dianjisj();

                $(".chakan").on('click',function(){
                    $(".xmguanli").attr("style","display:none;");
                    $(".glyshezhi").attr("style","display:none;");
                    $(".fenye").attr("style","display:none;");
                    $(".xinzenggs").attr("style","display:none;");
                    $(".down-2").attr("style","display:block;");
                    $(".down-1").attr("style","display:none;");
                    // $(".xmguanli_nr").attr("style","display:block;");
                    $(".xmguanli_nr_main").attr("style","display:block;");
                    $(".g_maincontent1_2").attr("style", "display:none;");
                    $(".g_maincontent1_3").attr("style", "display:none;");
                    $(".g_maincontent1_4").attr("style", "display:none;");
                    $(".g_maincontent1_5").attr("style", "display:none;");
                    $(".g_maincontent1_6").attr("style", "display:none;");
                    $(".g_maincontent1_7").attr("style", "display:none;");
                    $(".g_content_right_header").attr("style", "display:block;");
                    $(".g_maincontent1_1").attr("style", "display:block;");
                    $(".down").css("height", 968);

                    var self = $(this);
                    var app_id = self.parents("tr").find(".xiangmu_id").text();
                    var app_name = self.parents("tr").find(".xiangmu_name").text();
                    $(".y0 p").text(app_name);
                    console.log(app_id);

                    $(".btn-default").attr("name", app_id);

                    var startTime =  $("#date_start").val();
                    var endTime =   $("#date_end").val();
                    //新增用户和设备
                    interraction1_1(startTime,endTime,app_id);

                    //活跃玩家和设备
                    interaction1_2(startTime,endTime,app_id);

                    //付费金额
                    interaction1_3(startTime,endTime,app_id);

                    //付费用户
                    interaction1_4(startTime,endTime,app_id);

                    //应用下载量
                    interaction1_5(startTime,endTime,app_id);

                    //新增设备数量
                    interaction2_1(startTime,endTime,app_id);

                    //设备首次游戏时长
                    interaction2_2(app_id);

                    //设备地区分布
                    interaction2_3(app_id);

                    //新增用户数量
                    interaction3_1(startTime,endTime,app_id);

                    //用户首次使用时长
                    interaction3_2(app_id);

                    //用户地区分布
                    interaction3_3(app_id);

                    //日活跃设备
                    interaction4_1(startTime,endTime,app_id);

                    //周活跃设备
                    interaction4_2(startTime,endTime,app_id);

                    //月活跃设备
                    interaction4_3(startTime,endTime,app_id);

                    //设备已用天数
                    interaction4_4(app_id);

                    //日活跃用户
                    interaction5_1(startTime,endTime,app_id);

                    //周活跃用户
                    interaction5_2(startTime,endTime,app_id);

                    //月活跃用户
                    interaction5_3(startTime,endTime,app_id);

                    //用户已用天数
                    interaction5_4(app_id);

                    //设备留存
                    interaction6_1(startTime,endTime,app_id);

                    //用户留存
                    interaction6_2(startTime,endTime,app_id);

                    //操作系统
                    interaction7_1(startTime,endTime,app_id);

                    //联网方式
                    interaction7_2(startTime,endTime,app_id);

                    //运营商
                    interaction7_3(startTime,endTime,app_id);

                    //设备型号
                    interaction7_4(startTime,endTime,app_id);

                    huatu1_1_1(strnewuser,strnewdevice,strnewdate);
                    huatu1_1_3(money,strmdate);
                });

            }else console.log(data.status);

        }

    });
}

function addCompanyAndManager(){
    $.ajax({
        url:'a/company',
        type:'POST',
        data:{
            'name':$('#newcompanyname').val(),
            'company_describe':$('#newcompanydescribe').val(),
            'admin_name':$('#newadminname').val(),
            'admin_email':$('#newadminemail').val(),
            'admin_password':$('#newadminpassword').val(),
        },
        success:function(data){
            if(data.code==1){
               console.log(data);
            }else console.log(data.status);
        },


   })
}
function addApp(){
    console.log("new app click!");
    $.ajax({
        url:'a/add',
        type:'POST',
        data:{
            'name':$("#add-name").val(),
        'company':$("#gs_name").text(),
        'starttime':$("#add-starttime").val(),
        'endtime':$("#add-endtime").val(),
        'description':$("#add-description").val(),
        },
        success:function(data) {
            if (data.code == 1) {
                /*显示成功就可以了*/
                console.log("success "+data.data);

                var tr = $('#company_applist tr').length;

                $('#company_applist').append(" <tr> <td>" + tr + "</td> <td class='xiangmu_name' id='xiangmu_name" + tr + "'>" + $("#add-name").val() + "</td> <td id='xiangmu_id" + data.data + "'  class='xiangmu_id'>" + data.data  + "</td> <td class='start_time' id='start_time" + tr + "'>" + $("#add-starttime").val() + "</td> <td>" + $("#add-endtime").val() + "</td> <td class='descrip-tion'>" + $("#add-description").val() + "</td> <td> " +
                    "<button class='chakan'>查看</button> <button class='btn g2' data-toggle='modal' data-target='#myModa2'><p>修改</p> </button> <button class='app-delete'>删除</button> </td> </tr>")

                dianjisj();

            } else console.log(data.status);
        }

})
}
function getAppInfo(){
    $.ajax({
        url:'a/ainfo/id/'+$app_id,/*  $app_id 记得传值 */
        type:'GET',

        success:function(data){
            if(data.code==1){
                var list=data.data;
                var obj=JSON.parse(list);
/*
*   "data": "[{\"id\":1,\"name\":\"Angry Birds\",\"company\":\"wn\",\"description\":\"123\",\"starttime\":null,\"endtime\":null}]"
* */
            }else console.log(data.status);

        },
   })
}
function editApp(){
    $xiangm_id = $("#uuu").val() ;

    $.ajax({
        url:'a/edit',
        type:'POST',
        data:{
           'app_id':$("#uuu").val(),
          // 'app_id':$("#p_xiangmu_id").html();
            'name':$("#edit-name").val(),
            'endtime':$("#edit-endtime").val(),
            'description':$("#edit-description").val(),/*这些值如果没有变化就用原来的*/
        },
        success:function(data){

            if(data.code==1){
                /*显示成功就可以了*/
              $("#xiangmu_id"+$xiangm_id).siblings(".xiangmu_name").html($("#edit-name").val());
              $("#xiangmu_id"+$xiangm_id).siblings(".start_time").text($("#edit-endtime").val());
                $("#xiangmu_id"+$xiangm_id).siblings(".descrip-tion").text($("#edit-description").val());
            }else console.log(data.status);
        },


})

}
// function deleteApp(){
//     $id = $(".app-delete").siblings(".xiangmu_id").val();
//     console.log("qwer");
//     $.ajax({
//         url: 'a/delapp',
//         type: 'DELETE',
//         data: {
//             'app_id': $("#xiangmu_id"+$id).val(),
//         },
//         success: function (data) {
//             if (data.code == 1) {
//                 console.log(data);
//                 $id.parents("tr").remove();
//
//         }
//     },
// })  ;
// }
function dianjisj() {
    $(".app-delete").on("click",function () {
        var self=$(this);
        var xiangmu_id = self.parents("tr").find(".xiangmu_id").text();

        console.log(xiangmu_id);
        if (confirm("确定要删除该项目吗？"))
        {
                // var self=$(this);

                $.ajax({
                    url: 'a/delapp',
                    type: 'DELETE',
                    data: {
                        'app_id': xiangmu_id,
                    },
                    success: function (data) {
                        if (data.code == 1) {
                            console.log(data);
                            self.parents("tr").remove();

                        }
                    },
                })

            // self.parents("tr").remove();

        }else console.log(data.status);
    });
    $(".g1").on("click",function () {
        $("#add-name").val("");
        $("#add-starttime").val("");
        $("#add-endtime").val("");
        $("#add-description").val("");
    });
    // $(".chakan").on('click',function(){
    //     $(".xmguanli").attr("style","display:none;");
    //     $(".glyshezhi").attr("style","display:none;");
    //     $(".fenye").attr("style","display:none;");
    //     $(".xinzenggs").attr("style","display:none;");
    //     $(".down-2").attr("style","display:block;");
    //     $(".down-1").attr("style","display:none;");
    //     // $(".xmguanli_nr").attr("style","display:block;");
    //     $(".xmguanli_nr_main").attr("style","display:block;");
    //     $(".g_maincontent1_2").attr("style", "display:none;");
    //     $(".g_maincontent1_3").attr("style", "display:none;");
    //     $(".g_maincontent1_4").attr("style", "display:none;");
    //     $(".g_maincontent1_5").attr("style", "display:none;");
    //     $(".g_maincontent1_6").attr("style", "display:none;");
    //     $(".g_maincontent1_7").attr("style", "display:none;");
    //     $(".g_content_right_header").attr("style", "display:block;");
    //     $(".g_maincontent1_1").attr("style", "display:block;");
    //
    // });

    $(".g2").on("click",function () {
        $("#edit-name").val("");
        $("#edit-endtime").val("");
        $("#edit-description").val("");
        var self = $(this);
        var xiangmu_id = self.parents("tr").find(".xiangmu_id").text();
        var xiangmu_name = self.parents("tr").find(".xiangmu_name").text();
        var start_time = self.parents("tr").find(".start_time").text();
        $p_xiangmuid_html = '<input type="hidden" id="uuu" value="'+xiangmu_id+'" />'+xiangmu_id;
        $("#p_xiangmu_id").html($p_xiangmuid_html);
        $("#p_xiangmu_name").text(xiangmu_name);
        $("#p_start_time").text(start_time);
        // $("#edit-name").text() = "";
        // $("#edit-name").text() = "";
        // $("#edit-description").text() = "";
    });

    $("#xiugai_queding").on("click",function () {
        if($("#edit-name").val() == "" || $("#edit-endtime").val() == "" || $("#edit-description").val() == ""){
            alert("这三项为必填项内容");
        }
        else{
            var id = $("#p_xiangmu_id").val();
            var name = $("#edit-name").val();
            var endtime = $("#edit-endtime").val();
            var description = $("#edit-description").val();

            editApp();


            /*
             $(".xiangmu_id").each(function () {
             if($("#'xiangmu_id"+arr_data[i].id+"'").val() = id)
             {
             $("#'xiangmu_name" + arr_data[i].id + "'").val() = name;
             $("#'start_time"+arr_data[i].starttime+"'").val() = endtime;
             $("#'description"+arr_data[i].description+"'").val() = description;
             }else {

             };
             });
             */
            // $("#xiangmu_id".text()=id).parents("tr").find("#xiangmu_name").text();
            // $("#xiangmu_id".text()=id).parents("tr").find("#xiangmu_endtime").text();
            // $("#xiangmu_id".text()=id).parents("tr").find("#xiangmu_description").text();
            // // self.parents("tr").find("#xiangmu_name").text();
            // // self.parents("tr").find("#xiangmu_endtime").text();
            // // self.parents("tr").find("#xiangmu_description").text();
            // $("#xiangmu_name").text(name);
            // $("#xiangmu_endtime").text(endtime);
            // $("#xiangmu_description").text(description);

        }
    });
    // $(".app-delete").on("click",function () {
    //     var self = $(this).parent("tr");
    //     self.remove();
    // });

    // $(".app-delete").on("click",function () {
    //     if (confirm("确定要删除该项目吗？"))
    //     {
    //         //ajax 成功后执行
    //         var self = $(this).parents("tr");
    //         self.remove();
    //     }
    // })
    $(".biao2 button,.biao1 button,.biao3 button").mouseover(function(){
        $(this).css("color","black");
    });
    $(".biao2 button,.biao1 button,.biao3 button").mouseout(function(){
        $(this).css("color","#fff");
    });
}

