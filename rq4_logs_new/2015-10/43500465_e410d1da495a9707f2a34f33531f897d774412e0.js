

/* ���������װ�����ĺ���,����obj1Ϊ����Ԫ�أ�obj2Ϊ������  */
function trigger(obj1,obj2){
	/* ���������� */
	var gradualValue=0;
	/* �����������ʣ��˴��ɱ� */
	var sum=function(speed){
		gradualValue+=speed;
	};
	/* �����ӳٵ���ID */
	var timeroverID=null;
	var timeroutID=null;
	/* ������ֵ��ֵ��ѡ���Ķ�����ʽ���ԣ��˺�������ʽ�ڿ�Ϊ��չ��������ʽ���Ը�ֵ */
	var property=function(method){
		switch(method){
			case "toggleWidth":
				obj1.style.width=gradualValue+"px";
				break;
			case "toggleHeight":
				obj1.style.height=gradualValue+"px";
				break;
			case "toggleMarginLeft":
				obj1.style.marginLeft=gradualValue+"px";
				break;
            case "toggleMarginTop":
				obj1.style.marginTop=gradualValue+"px";
				break;
			case "toggleOpacity":
				obj1.style.opacity=gradualValue;
				break;	
            case "toggle_Opacity":
				obj1.style.opacity=1-gradualValue;
				break;
            case "toggleRotate":
				obj1.style.transform="rotate("+gradualValue+"deg)";
				break;				
            case "toggleScale":
				obj1.style.transform="scale("+gradualValue+")";
				break;								
		};		
	};
	/* ������봦������ */
    var overProgress=function(Fparas,Lparas,granularity,frequentness,method){
		return function(){
			clearTimeout(timeroutID);
		    (function(){
			    if(gradualValue<Lparas){
			        sum(granularity);
			        property(method);
			        timeroverID=setTimeout(arguments.callee,frequentness);
		        } else {
			    	gradualValue=Lparas;
			    	property(method);
			    };
		    }());
        };		
	};	
	/* ����Ƴ��������� */
	var outProgress=function(Fparas,Lparas,granularity,frequentness,method){
		return function(){
			clearTimeout(timeroverID);
			(function(){
				if(gradualValue>Fparas){
				    sum(-granularity);
				    property(method);
				    timeroutID=setTimeout(arguments.callee,frequentness);
			    } else {
			    	gradualValue=Fparas;
			    	property(method);
			    };
			}());			
		};
	};
	/* ��괥���������� */
	var toggleProgress=function(method){
		return function(Fparas,Lparas,granularity,frequentness){
		    gradualValue=Fparas;
		    obj2.onmouseover=overProgress(Fparas,Lparas,granularity,frequentness,method);
	        obj2.onmouseout=outProgress(Fparas,Lparas,granularity,frequentness,method);
			obj1.onmouseover=overProgress(Fparas,Lparas,granularity,frequentness,method);
	        obj1.onmouseout=outProgress(Fparas,Lparas,granularity,frequentness,method);
		};
	};
	/* ���������ӿڣ���������չ */
	return {
		toggleWidth:toggleProgress("toggleWidth"),
		toggleHeight:toggleProgress("toggleHeight"),
		toggleMarginLeft:toggleProgress("toggleMarginLeft"),
		toggleMarginTop:toggleProgress("toggleMarginTop"),
		toggleOpacity:toggleProgress("toggleOpacity"),
		toggle_Opacity:toggleProgress("toggle_Opacity"),
		toggleRotate:toggleProgress("toggleRotate"),
		toggleScale:toggleProgress("toggleScale")		
	};	
};





/* 
trigger.js�÷�˵��:
trigger()��Ҫ������������obj1��obj2����һ������Ҫ���Ըı��Ԫ�أ��ڶ����Ǵ���Ԫ�ء�
trigger.js��Ҫ����ʵ��HTML��Ԫ�ص������ͣ������Ч������ΪԪ����������8�ַ���:
  1.  toggleHeight() �������� �˷������Դ���Ԫ�صĸ߶����ԣ�
  2.  toggleWidth() �������� �˷������Դ���Ԫ�صĿ������ԣ�
  3.  toggleOpacity() �������� �˷������Դ���Ԫ�صĲ�͸�������ԣ�
  4.  toggle_Opacity() �������� �˷������Դ���Ԫ�ص�͸�������ԣ�
  5.  toggleMarginLeft() �������� �˷������Դ���Ԫ�ص�����߾����ԣ�
  6.  toggleMarginTop() �������� �˷������Դ���Ԫ�ص�����߾����ԣ�
  7.  toggleScale() �������� �˷������Դ���Ԫ�ص��������ԣ�
  8.  toggleRotate() �������� �˷������Դ���Ԫ�ص���ת���ԣ�
  ��8�ַ�������Ҫ����4��������Fparas,Lparas,granularity,frequentness�����У�FparasΪ���Ա仯ǰ��ֵ��LparasΪ���Ա仯���ֵ��granularityΪ���Ա仯���ȣ�frequentnessΪ���Ա仯��֡ʱ����

����
  HTML��
  <!doctype html>
    <html>
      <head>
        <meta charset="utf-8">
	    <title>reuse demo</title>
	    <script src="trigger.js"></script>
      </head>
      <body>
        <div style="width:100px;height:100px;background-color:red"></div>
    	<p>trigger</p>
      </body>
    </html>  
  
  JS��
  window.onload=function(){
	test();
  };
  function test(){
	var odiv=document.getElementsByTagName("div")[0];
	var otrigger=document.getElementsByTagName("p")[0];
	trigger(odiv,otrigger).toggle_Opacity(0,1,0.02,20);
  }; 
   */
