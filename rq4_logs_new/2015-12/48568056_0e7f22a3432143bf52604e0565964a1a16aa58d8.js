/**
 * Created by apple on 15/12/25.
 */
//系统confirm 定制
/*参数
 基于bootstrap 3,jquery 2.0开发
 msg:必须 带给本函数的展示信息
 okFunc:必须 确定后回调函数
 cancelFunc:必须 取消后回调函数
 ext:可选 带给回调函数的参数信息 类型任意
 */
var Jason = {};
Jason.confirm = function(msg,okFunc,cancelFunc,ext){
    ///*create modal*/
    var modal = document.createElement("div");
    modal.className = "modal fade";
    modal.id = "sysConfirm";
    modal.setAttribute("data-backdrop","static");
    //create modal-dialog
    var modalDialog = document.createElement("div");
    modalDialog.className = "modal-dialog";
    // create modal-content
    var modalContent = document.createElement("div");
    modalContent.className = "modal-content";
    /**********************
     create modal-header
     * *******************/
    var modalHeader = document.createElement("div");
    modalHeader.className = "modal-header";
    var closeButton = document.createElement('button');
    closeButton.type = "button";
    closeButton.className = "close";
    closeButton.setAttribute("data-dismiss","modal");
    closeButton.setAttribute("aria-label","Close");
    closeButton.onclick = function(){
        cancelFunc(ext);
        $("#sysConfirm").modal("hide");
        document.querySelector("#sysConfirm").remove();
    }
    var closeIcon = document.createElement("span");
    closeIcon.setAttribute("aria-hidden","true");
    closeIcon.innerHTML = "&times";
    closeButton.appendChild(closeIcon);
    modalHeader.appendChild(closeButton);
    var title = document.createElement("h4");
    title.className="modal-title";
    var titleText = document.createTextNode("系统提示");
    title.appendChild(titleText);
    modalHeader.appendChild(title);
    // create modal-header  end

    /**********************
     create modal-body
     * *******************/
    var modalBody = document.createElement("div");
    modalBody.className="modal-body";
    modalBody.id="sysConfirmBody";
    var modalText = document.createTextNode(msg);
    modalBody.appendChild(modalText);
    //create modal-body end

    /**********************
     * create footer
     * *******************/
    var modalFooter = document.createElement("div");
    modalFooter.className = "modal-footer";
    modalFooter.id="sysConfirmFoot";

    /*create ok btn*/
    var okText = document.createTextNode("确定");
    var ok = document.createElement("div");
    ok.className = "btn btn-primary";
    ok.onclick = function(){
        okFunc(ext);
        $("#sysConfirm").modal("hide");
        document.querySelector("#sysConfirm").remove();
    }
    ok.appendChild(okText);

    /*create cancel btn*/
    var cancelText = document.createTextNode("取消");
    var cancel = document.createElement("div");
    cancel.className = "btn btn-default";
    cancel.onclick = function(){
        cancelFunc(ext);
        $("#sysConfirm").modal("hide");
        document.querySelector("#sysConfirm").remove();
    }
    cancel.appendChild(cancelText);
    modalFooter.appendChild(ok);
    modalFooter.appendChild(cancel);

    /*装载*/
    modalContent.appendChild(modalHeader);
    modalContent.appendChild(modalBody);
    modalContent.appendChild(modalFooter);
    modalDialog.appendChild(modalContent);
    modal.appendChild(modalDialog);
    document.querySelector("body").appendChild(modal);
    $("#sysConfirm").modal();

}