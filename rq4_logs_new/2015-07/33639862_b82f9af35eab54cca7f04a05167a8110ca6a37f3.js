/**
 * Created by jack on 15-03-27.
 * main Controller used by Terminal app
 */
Ext.define('PatientApp.controller.Doctor', {
    extend: 'Ext.app.Controller',
    config: {
        views: [
            'doctor.Doctors',
            'doctor.DoctorsMessage'

        ],
        models: [
            'doctor.Doctor',
            'doctor.DoctorMessage'

        ],
        stores: [

            'doctor.Doctors',
            'doctor.DoctorMessages'

        ],
        maxPosition: 0,
        scroller: null,
        control: {
            doctorsnavview: {
                push: 'onMainPush'
            },
            'doctormessagelistview': {

                /*initialize: function (list) {
                    var me = this,
                        scroller = list.getScrollable().getScroller();

                    scroller.on('maxpositionchange', function (scroller, maxPos, opts) {
                        me.setMaxPosition(maxPos.y);
                    });
                    //console.log(scroller);
                    //testobj=list;
                    me.setScroller(scroller);

                    //me.getMessage().setValue(Ext.create('Chat.ux.LoremIpsum').getSentence());
                },*/
                touchend:'voicetouchend',
                touchstart:'voicetouchbegin'
            },
            doctorsview: {
                itemtap: 'onDoctorSelect',
                itemtaphold:'onDoctorHold',
                viewshow:'listShow'
            },
            sendmessagebtn:{
                tap:'sendMessageControler'
            }
            ,
            choosepicbtn:{
                tap:'doImgCLick'
            },
            makevideobtn:{
                tap:'makevideobtn'
            }

        },
        refs: {
            doctorsview: '#doctorsnavigationview #doctorlist',
            mainview: 'main',
            doctormessagelistview:'doctormessagelist',
            messagecontent: '#doctorsnavigationview #messagecontent',
            choosepicbtn: '#doctorsnavigationview #choosepic',
            sendmessagebtn: '#doctorsnavigationview #sendmessage',
            makevideobtn: '#doctorsnavigationview #makevideo',
            patientsview: '#patientsnavigationview #patientlist',
            doctorsnavview:'main #doctorsnavigationview'
        }
    },
    voicetouchbegin:function(item){
        if(!this.voiceoverlay){
            this.voiceoverlay = Ext.Viewport.add({
                xtype: 'panel',

                // We give it a left and top property to make it floating by default
                /*left: 0,
                top: 0,*/
                centered:true,

                // Make it modal so you can click the mask to hide the overlay
                modal: true,
                hideOnMaskTap: true,

                // Make it hidden by default
                hidden: true,

                // Set the width and height of the panel
                width: 200,
                height: 140,

                // Here we specify the #id of the element we created in `index.html`
                contentEl: 'content',

                // Style the content and make it scrollable
                styleHtmlContent: true,
                scrollable: true,

                // Insert a title docked at the top with a title
                items: [
                    {
                        //docked: 'top',
                        xtype: 'panel',
                        html:'<div id="voicerecord">正在录音,松开发送...</div>',
                        title: 'Overlay Title'
                    }
                ]
            });

        }

        this.voiceoverlay.show();
        this.makerecording();



    },

    makerecording:function(){
        var me=this;
        window.requestFileSystem(LocalFileSystem.PERSISTENT, 0,gotFS , function(){});
         function gotFS(fileSystem) {

            fileSystem.root.getFile("blank.mp3", {create: true, exclusive: false}, gotFileEntry,  function(){

         });

         function gotFileEntry(fileEntry) {

             me.voicefileentry=fileEntry;
             me.voicerecordsrc=fileEntry.toInternalURL();
             //Ext.Msg.alert("filepath",src);

             me.mediaRec = new Media(me.voicerecordsrc,
             // success callback
             function() {
             //Ext.Msg.alert("success","recordAudio():Audio Success");
             },

             // error callback
             function(err) {
             //console.log("recordAudio():Audio Error: "+ err.code);
             Ext.Msg.alert("success","recordAudio():Audio Success"+ err.code);
             });

             // Record audio
             me.mediaRec.startRecord();

             setTimeout(function() {


             }, 3000);



         }
         }



    },

    voicetouchend:function(item){
        var me=this;
        this.voiceoverlay.hide();

        //Ext.Msg.alert('end', me.voicerecordsrc, Ext.emptyFn);
        this.mediaRec.stopRecord();
        me.mediaRec.release();

        //me.getMessagecontent().setValue('<audio  src="'+me.voicerecordsrc+'" controls>')  ;

        var btn=item.down('#sendmessage');

        btn.isfile=true;
        btn.filetype='voice';
        btn.fileurl=me.voicerecordsrc;

        me.sendMessageControler(btn);





    },

    makevideobtn:function(btn){


        var me=this;
        var patientController=this.getApplication().getController('Patient');
        me.applyfordoctor(btn,Ext.bind(patientController.makevideobtn, me));

    },
    doImgCLick: function (item) {
        var list=item.up('list');
        var btn=list.down('#sendmessage');
        testobj=btn;
        var me = this;
        var actionSheet = Ext.create('Ext.ActionSheet', {
            items: [
                {
                    text: '相机拍照',
                    handler: function () {
                        //alert(1);

                        imagfunc('camera');
                    }
                    //ui  : 'decline'
                },
                {
                    text: '图片库',
                    handler: function () {
                        //alert(2);
                        imagfunc('library');
                    }
                },
                {
                    text: '取消',
                    handler: function () {

                        actionSheet.hide();
                    },
                    ui: 'decline'
                }
            ]
        });

        Ext.Viewport.add(actionSheet);
        actionSheet.show();

        var imagfunc = function (type) {
            actionSheet.hide();
            //var imgpanel = me.getImgpanel();
            //alert(1);
            Ext.device.Camera.capture({
                source: type,
                destination: 'file',
                success: function (imgdata) {

                    //var srcdata="data:image/png;base64,"+imgdata;
                    //me.getMessagecontent().setValue('<img height="200" width="200" src="'+imgdata+'">')  ;
                    btn.isfile=true;
                    btn.filetype='image';
                    btn.fileurl=imgdata;

                    me.sendMessageControler(btn);

                }
            });
        };


    },

    continueAsk:function(btn){
        var me=this;
        var listview=btn.up('list');
        var myinfo= listview.mydata;

        var toinfo=listview.data;
        var successFunc = function (response, action) {

            var res=JSON.parse(response.responseText);

            if(res.success){

                Ext.Msg.show({
                    title:'成功',
                    message: '继续问诊',
                    buttons: Ext.MessageBox.OK,
                    fn:function(){
                        me.sendMessageControler(btn);
                    }
                });


            }else{
                Ext.Msg.alert('失败', res.message,function(){});
            }

        };
        var failFunc=function(response, action){
            Ext.Msg.alert('失败', '服务器连接异常，请稍后再试', function(){

            });
            //Ext.Msg.alert('test', 'test', Ext.emptyFn);
        }
        var url="patient/continuewithapply";
        var params={
            userid:myinfo._id,
            doctorid:toinfo.get("_id")
        };
        CommonUtil.ajaxSend(params,url,successFunc,failFunc,'POST');


    },
    aliback:function(btn){
        var listview=btn.up('list');
        var myinfo= listview.mydata;

        var toinfo=listview.data;
        var me=this;

        //Ext.Msg.alert('提示', '这里模拟支付宝退款接口', function(){
            //makemoneybyuserid

            var successFunc = function (response, action) {

                var res=JSON.parse(response.responseText);

                if(res.success){

                    Ext.Msg.show({
                        title:'成功',
                        message: '费用已退回',
                        buttons: Ext.MessageBox.OK,
                        fn:Ext.emptyFn
                    });


                }else{
                    Ext.Msg.alert('失败', res.message,function(){

                    });
                }

            };
            var failFunc=function(response, action){
                Ext.Msg.alert('失败', '服务器连接异常，请稍后再试', function(){

                });
                //Ext.Msg.alert('test', 'test', Ext.emptyFn);
            }
            var url="patient/backmoneybyuseridwithapply";
            var params={
                userid:myinfo._id,
                doctorid:toinfo.get("_id")
            };
            CommonUtil.ajaxSend(params,url,successFunc,failFunc,'POST');

        //});


    },



    alipay:function(btn){

        var settingsController=this.getApplication().getController('Settings');
        settingsController.showAddMoneyForm(btn);
       /* var listview=btn.up('list');
        var myinfo= listview.mydata;

        var toinfo=listview.data;
        var me=this;

        Ext.Msg.alert('提示', '这里模拟支付宝支付接口', function(){
            var successFunc = function (response, action) {
                var res=JSON.parse(response.responseText);
                if(res.success){
                    Ext.Msg.show({
                        title:'成功',
                        message: '现在可以问诊了',
                        buttons: Ext.MessageBox.OK,
                        fn:Ext.emptyFn //任务描述这里可以输入默认信息
                    });

                }else{
                    Ext.Msg.alert('失败', res.message,function(){

                    });
                }

            };
            var failFunc=function(response, action){
                Ext.Msg.alert('失败', '服务器连接异常，请稍后再试', function(){

                });
                //Ext.Msg.alert('test', 'test', Ext.emptyFn);
            }
            var url="patient/makemoneybyuseridwithapply";
            var params={
                userid:myinfo._id,
                money:20,
                doctorid:toinfo.get("_id")
            };
            CommonUtil.ajaxSend(params,url,successFunc,failFunc,'POST');

        });*/

    },
    applyforpay:function(myinfo,toinfo,btn){
        var me=this;
        Ext.Msg.confirm('消息','是否现在挂号?',function(buttonId){

            if(buttonId=='yes'){

                var successFunc = function (response, action) {

                    var res=JSON.parse(response.responseText);

                    if(res.success){

                        Ext.Msg.show({
                            title:'成功',
                            message: '现在可以问诊了',
                            buttons: Ext.MessageBox.OK,
                            fn:Ext.emptyFn
                        });
                        //Ext.Msg.alert('成功', '已接受推荐，等待对方同意', Ext.emptyFn);

                    }else{
                        Ext.Msg.alert('失败', res.message,function(){

                            var actionSheet = Ext.create('Ext.ActionSheet', {
                                items: [
                                    {
                                        text: '支付宝支付',
                                        handler:function(){
                                            me.alipay(btn);
                                            actionSheet.hide();
                                        }
                                    },

                                    {
                                        text: '取消',
                                        handler : function() {
                                            actionSheet.hide();
                                        },
                                        ui  : 'confirm'
                                    }
                                ]
                            });

                            Ext.Viewport.add(actionSheet);
                            actionSheet.show();

                        });
                    }

                };
                var failFunc=function(response, action){
                    Ext.Msg.alert('失败', '服务器连接异常，请稍后再试', function(){


                    });
                    //Ext.Msg.alert('test', 'test', Ext.emptyFn);
                }
                var url="patient/makeapplyfordoctor";
                var params={
                    patientid:myinfo._id,
                    doctorid:toinfo.get("_id")
                };
                CommonUtil.ajaxSend(params,url,successFunc,failFunc,'POST');
            }else{
                //var view=me.getDoctorsnavview();
                //view.pop();
            }


        });
    },
    applyfordoctor:function(btn,callback){
        var listview=btn.up('list');
        var myinfo= listview.mydata;
        var applytimelabel=listview.down('label');

        var toinfo=listview.data;
        var me=this;
        var successFunc = function (response, action) {

            var res=JSON.parse(response.responseText);
            if(res){
                //Ext.Msg.alert('成功', '推荐医生成功', Ext.emptyFn);\
                console.log(res);
                var t=CommonUtil.getovertime(res.applytime);
                if(t<=0){
                    if(res.nums==0){
                        var actionSheet = Ext.create('Ext.ActionSheet', {
                            items: [
                                {
                                    text: '继续问诊(已挂号)',
                                    handler:function(){
                                        //me.showPatientList(record);
                                        me.continueAsk(btn);
                                        actionSheet.hide();
                                    }
                                },

                                {
                                    text: '我要退款',
                                    disabled:res.isreply,
                                    handler : function() {
                                        me.aliback(btn);
                                        actionSheet.hide();
                                    },
                                    ui  : 'confirm'
                                }
                            ]
                        });

                        Ext.Viewport.add(actionSheet);
                        actionSheet.show();
                    }else{
                        me.applyforpay(myinfo,toinfo,btn);
                    }

                }else{
                    var timecallback=function(t,asktimeinterval){
                        if(t<=0){
                            clearInterval(asktimeinterval);
                            me.sendMessageControler(btn);
                            me.showDoctosView({fromid:toinfo.get("_id")});

                        }else{
                            var m=Math.floor(t/1000/60%60);
                            var s=Math.floor(t/1000%60);
                            applytimelabel.setHtml('<div>问诊时间剩余:'+m + "分 "+s + "秒"+'</div>');
                            applytimelabel.show();
                        }

                    };
                    CommonUtil.lefttime(timecallback,res.applytime,toinfo.get("_id"));
                    callback(btn);
                }

            }else{
                  me.applyforpay(myinfo,toinfo,btn);
            }

        };
        var failFunc=function(response, action){
            Ext.Msg.alert('失败', '服务器连接异常，请稍后再试', Ext.emptyFn);
        }
        var url="patient/applyfordoctor";

        var params={
            patientid:myinfo._id,
            doctorid:toinfo.get("_id")

        };
        CommonUtil.ajaxSend(params,url,successFunc,failFunc,'GET');
    },

    showDoctosView:function(message){
        var mainView=this.getMainview();
        mainView.setActiveItem(1);

        var listView=this.getDoctorsview();
        var store=listView.getStore();
        var index =this.filterReceiveIndex(message,store);
        listView.select(index);
        listView.fireEvent('itemtap',listView,index,listView.getActiveItem(),store.getAt(index));

    },

    sendMessageControler:function(btn){
        var me=this;
        me.applyfordoctor(btn,Ext.bind(me.sendMessage, me));
    },
    scrollMsgList:function(){
        var scroller=this.getScroller();

        var task = Ext.create('Ext.util.DelayedTask', function() {
            scroller.scrollToEnd(true);
        });
        task.delay(500);

    },
    sendMessage:function(btn){
        var me=this;
        //var content=Ext.String.trim(this.getMessagecontent().getValue());
        var message=btn.up('list').down('#messagecontent');
        var content = Ext.String.trim(message.getValue());

        if((content&&content!='')||btn.isfile){
            var listview=btn.up('list');
            var myinfo= listview.mydata;

            var toinfo=listview.data;
            var imgid='chatstatusimg'+(new Date()).getTime();
            var message=Ext.apply({message:content}, myinfo);
            //console.log(imgid);
            if(!btn.isfile)listview.getStore().add(Ext.apply({local: true,imgid:imgid}, message));

            //this.scrollMsgList();

            var mainController=this.getApplication().getController('Main');

            var socket=mainController.socket;

            /*Ext.Msg.alert('tip',JSON.stringify({
                type:"doctorchat",

                to :toinfo.get("_id")
            }));*/
            if(btn.isfile){

                var win = function (r) {
                    //Ext.Msg.alert('seccess',r.response);
                    var res=JSON.parse(r.response);
                    var messagestr="";
                    if(btn.filetype=='image'){
                        messagestr='<img height="200" width="200" src="'+Globle_Variable.serverurl+'files/'+res.filename+'">';
                    }else if(btn.filetype=='voice'){
                        messagestr='<audio  src="'+Globle_Variable.serverurl+'files/'+res.filename+'" controls>';
                    }
                    message.message=messagestr;
                    listview.getStore().add(Ext.apply({local: true,imgid:imgid}, message));

                    //me.scrollMsgList();

                    socket.send(JSON.stringify({
                        type:"doctorchat",
                        from:myinfo._id,
                        fromtype:0,
                        imgid:imgid,
                        ctype:btn.filetype,
                        to :toinfo.get("_id"),
                        content: res.filename
                    }));

                    me.voicefileentry.remove(function(){},function(){});

                }

                var fail = function (error) {
                    me.voicefileentry.remove(function(){},function(){});
                    //Ext.Msg.alert('error',"An error has occurred: Code = " + error.code);

                }

                var options = new FileUploadOptions();
                options.fileKey = "file";
                options.fileName = btn.fileurl.substr(btn.fileurl.lastIndexOf('/') + 1);

                var ft = new FileTransfer();
                //Ext.Msg.alert('seccess',Globle_Variable.serverurl+'common/uploadfile');
                ft.upload(btn.fileurl, encodeURI(Globle_Variable.serverurl+'common/uploadfile'), win, fail, options);

                btn.isfile=false;


            }else{
                socket.send(JSON.stringify({
                    type:"doctorchat",
                    from:myinfo._id,
                    fromtype:0,
                    imgid:imgid,
                    to :toinfo.get("_id"),
                    content: content
                }));

            }





        }else{
            CommonUtil.showMessage("no content",true);
        }


    },

    recommendConfirmProcess:function(recommend,e){
        var me = this;
        try {
            //Ext.Msg.alert('test', cordova.plugins.notification.local.schedule , Ext.emptyFn);
            cordova.plugins.notification.local.schedule({
                //id: recommend._id,
                id:me.messageid,
                title: "新患者",
                text:  patientinfo.realname ,

                //firstAt: monday_9_am,
                //every: "week",
                //sound: "file://sounds/reminder.mp3",
                //icon: "http://icons.com/?cal_id=1",
                data: {data: recommend,type:'recommendconfirm'}
            });
            me.messageid++;
            /*cordova.plugins.notification.local.on("click", function (notification) {

             me.receiveQuickApplyShow(notification.data.data, e);

             });*/

        } catch (err) {

            me.receiverecommendConfirmShow(recommend, e);

        } finally {


        }

    },

    receiverecommendConfirmShow:function(recommend, e){
        var mainView = this.getMainview();
        mainView.setActiveItem(1);
       this.initDoctorList();
    },

    receiveRecommendProcess:function(data,e){
        //console.log(data);
        //alert("1");
        for(var i=0;i<data.length;i++){
            //alert(i);
            var recommend=data[i];
            //message.message=message.content;
            this.receiveRecommendNotification(recommend,e);
        }
        //listView.select(1);
    },

    receiveRecommendNotification:function(recommend,e){
        var me=this;
        try {

            //Ext.Msg.alert('test', cordova.plugins.notification.local.schedule , Ext.emptyFn);
            cordova.plugins.notification.local.schedule({
                //id: recommend._id ,
                id:me.messageid,
                title: recommend.rectype==1?("医生:"+recommend.frominfo.userinfo.realname+"推荐的"):
                    ("患者:"+recommend.frominfo.realname+"推荐的"),
                text: "新医生:"+recommend.doctorinfo.userinfo.realname,
                //firstAt: monday_9_am,
                //every: "week",
                //sound: "file://sounds/reminder.mp3",
                //icon: "http://icons.com/?cal_id=1",
                data: { data:recommend,type:'recommend'}
            });
            me.messageid++;
            /*cordova.plugins.notification.local.on("click", function (notification) {

                me.receiveRecommendShow(recommend,e);

            });*/

        }catch (err){

            me.receiveRecommendShow(recommend,e);

        } finally{


        }


    },
    receiveRecommendShow:function(recommend,e){
        //alert(1);
        //console.log(recommend);
        Ext.Msg.confirm('消息','是否添加'+ (recommend.rectype==1?"医生:"+recommend.frominfo.userinfo.realname+"推荐":
        "患者:"+recommend.frominfo.realname+"推荐")+"的医生:"+recommend.doctorinfo.userinfo.realname,function(buttonId){

            if(buttonId=='yes'){

                var successFunc = function (response, action) {

                    var res=JSON.parse(response.responseText);

                    if(res.success){

                        Ext.Msg.show({
                            title:'成功',
                            message: (recommend.isdoctoraccepted||recommend.ispatientaccepted)?'已成功添加医生':
                                '已接受推荐，等待对方同意',
                            buttons: Ext.MessageBox.OK,
                            fn:Ext.emptyFn
                        });
                        //Ext.Msg.alert('成功', '已接受推荐，等待对方同意', Ext.emptyFn);

                    }else{
                        Ext.Msg.alert('失败', '服务器连接异常，请稍后再试', Ext.emptyFn);
                    }

                };
                var failFunc=function(response, action){
                    Ext.Msg.alert('失败', '服务器连接异常，请稍后再试', Ext.emptyFn);
                    //Ext.Msg.alert('test', 'test', Ext.emptyFn);
                }
                var url="patient/acceptrecommend";
                var params={
                    rid:recommend._id
                };
                CommonUtil.ajaxSend(params,url,successFunc,failFunc,'POST');
            }else{
                //var view=me.getDoctorsnavview();
                //view.pop();
            }


        });



    },
    onDoctorHold:function(list,index, target, record, e) {
        //long patient hold
        var me=this;
        list.lastTapHold = new Date();

        var actionSheet = Ext.create('Ext.ActionSheet', {
            items: [
                {
                    text: '推荐患者',
                    handler:function(){
                        me.showPatientList(record);
                        actionSheet.hide();
                    }
                },

                {
                    text: '取消',
                    handler : function() {
                        actionSheet.hide();
                    },
                    ui  : 'confirm'
                }
            ]
        });

        Ext.Viewport.add(actionSheet);
        actionSheet.show();

    },
    onMainPush: function (view, item) {
        //this.getDoctorsnavview().deselectAll();
    },
    listShow:function(){
        //this.initPatientList();
        //Ext.Msg.alert('侧额额', 'cess 说', Ext.emptyFn);
    },
    messageView:{},
    onDoctorSelect: function (list, index, node, record) {

        if (!list.lastTapHold || ( new Date()-list.lastTapHold  > 1000)) {
            console.log(record);

            if (!this.messageView[record.get('_id')]){
                this.messageView[record.get('_id')] =Ext.create('PatientApp.view.doctor.DoctorsMessage');

            }
            var selectview=this.messageView[record.get('_id')];
            testobj=this;

            selectview.setTitle(record.get('userinfo').realname);
            selectview.data=record;
            selectview.mydata=Globle_Variable.user;

            console.log("this.getDoctorsnavview().getItems()");
            console.log(this.getDoctorsnavview().getItems());
            testobj=this.getDoctorsnavview();

            this.getDoctorsnavview().push(selectview);


        }



        // Push the show contact view into the navigation view

    },
    receiveMessageProcess:function(data,e){
        for(var i=0;i<data.length;i++){
            var message=data[i];
            message.message=message.content;
            if(message.type=='image'){
                message.message='<img height="200" width="200" src="'+Globle_Variable.serverurl+'files/'+message.content+'">';
            }else if(message.type=='voice'){
                message.message='<audio  src="'+Globle_Variable.serverurl+'files/'+message.content+'" controls>';
            }
            //console.log(1111111111);
            //console.log(message);
            this.receiveMessageNotification(message,e);
        }
        //listView.select(1);
    },
    messageid:0,
    receiveMessageNotification:function(message,e){

        var me=this;
        try {

            if(Globle_Variable.isactived){
                me.receiveMessageShow(message,e);

            }else{

                (function(message,cid){

                    cordova.plugins.notification.local.schedule({
                        //id: message._id,
                        id: cid,
                        title: (message.fromtype==0?'病友 ':'医生 ')+ message.userinfo.realname+' 来消息啦!' ,
                        text: message.message,
                        //firstAt: monday_9_am,
                        //every: "week",
                        //sound: "file://sounds/reminder.mp3",
                        //icon: "http://icons.com/?cal_id=1",
                        data: { data: message }
                    });


                } )(message,me.messageid)  ;
                me.messageid++;

            }



        }catch (err){
            console.log("33333333333333") ;
           me.receiveMessageShow(message,e);

        } finally{


        }


    },

    messageshowfinal:function(message){
        var messagestore=null;
        var doctorController=this.getApplication().getController('Doctor');
        var patientController=this.getApplication().getController('Patient');

        if(message.fromtype==0){
            messagestore=patientController.messageView[message.fromid].getStore();
        }else{
            //Ext.Msg.alert('clicked event',JSON.stringify(message));
            messagestore=doctorController.messageView[message.fromid].getStore();
        }
        //Ext.Msg.alert('store added', 'is clicked');
        messagestore.add(Ext.apply({local: false}, message));

    },
    receiveMessageShow:function(message,e){
        var me=this;
        try{

            var mainView=this.getMainview();
            var listView=null;
            var messagestore=null;

            if(message.fromtype==0){

                mainView.setActiveItem(0);
                listView=this.getPatientsview();


            }else{
                mainView.setActiveItem(1);
                listView=this.getDoctorsview();
            }
            var store=listView.getStore();

            var flag=true;
            //console.log(store.data);
            var index=0;
            for(var i=0;i<store.data.items.length;i++){
                var fromid=message.fromtype==1?store.data.items[i].get('_id'):store.data.items[i].get('patientinfo')._id
                if(message.fromid==fromid){
                    flag=false;
                    index=i;
                    break;
                }
            }
            if(flag){
                //message.userinfo.realname="<div style='color: #176982'>(New)</div>"+message.userinfo.realname;
                //store.insert(0,[message]);
                //index=store.data.items.length;
                message._id=message.fromid;
                store.add(message);
                index =me.filterReceiveIndex(message,store);

            }



            //var index =this.filterReceiveIndex(message,store);
            var nav =listView.getParent();



            if(nav.getItems().length==3&&message.fromid!=(message.fromtype==1?nav.getActiveItem().data.get('_id'):nav.getActiveItem().data.get('patientinfo')._id)){
                nav.pop();
                setTimeout(function(){
                    try{
                        listView.select(index);
                        listView.fireEvent('itemtap',listView,index,listView.getActiveItem(),store.getAt(index),e);

                    }catch(err){

                    }finally{

                        me.messageshowfinal(message);
                    }

                },500);
            }
            else{
                listView.select(index);
                listView.fireEvent('itemtap',listView,index,listView.getActiveItem(),store.getAt(index),e);

            }
            //alert(1);

        }catch(err) {

        }finally{
            this.messageshowfinal(message);

        }
    },
    filterReceiveIndex:function(data,store){
        var listdata=store.data.items;
        var index=0;
        for(var i=0;i<listdata.length;i++){
            if(listdata[i].get("_id")==data.fromid){
                index=i;
                break;
            }
        }
        return index;
    },
    initDoctorList:function(){

        var store=Ext.getStore('Doctors');
        store.load({
            //define the parameters of the store:
            params:{
                patientid : Globle_Variable.user._id
            },
            scope: this,
            callback : function(records, operation, success) {

            }});

    }


});