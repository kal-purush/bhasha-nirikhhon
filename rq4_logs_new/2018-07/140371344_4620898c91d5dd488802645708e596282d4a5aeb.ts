module game {
	export class RandomTest extends eui.Component implements eui.UIComponent{
		private m_start:eui.Label;
		private m_num1:imageItem;
		private m_num2:imageItem;
		private m_num0:imageItem;
		public constructor() {
			super();
			this.addEventListener(eui.UIEvent.COMPLETE,this.onComplete,this);
			this.skinName = "resource/eui_skins/RandomTestSkin.exml";
		}
		private onComplete(){
			this.addEvent();
		}
		protected childrenCreated(){ 
			super.childrenCreated();
			
		}
		private addEvent(){
			this.m_start.once(egret.TouchEvent.TOUCH_TAP,this.startGame,this)
		}
		private startGame(){//3~7  3,4,5,1,2
			console.log("开始游戏:高",this.m_num1.height,"宽----",this.m_num1.width,"y----",this.m_num1.y);//开始游戏:高 1298 宽---- 126 y---- 0
			this.m_num0.y = 0;
			this.m_num1.y = 0;
			this.m_num2.y = 0;
			this.targetAnimation(5,this.m_num0,this.getRandomPos(2),500);
			this.targetAnimation(8,this.m_num1,this.getRandomPos(2),200);
			this.targetAnimation(7,this.m_num2,this.getRandomPos(2),400);

		}
		private getRandomPos(x:number){
			return (x-1)*112-45.5
		}
		private targetAnimation(times=3,target:egret.DisplayObject,targetY,speed){
			 target.y = 0
			 times--;
			 if(times==0){
				 egret.Tween.get(target).to({y:0-targetY},1500).call(()=>{
					 console.log("结束，",targetY)
					 this.m_start.once(egret.TouchEvent.TOUCH_TAP,this.startGame,this)
				});
			 }else{
				egret.Tween.get(target).to({y:0-target.height/2},speed).call(this.targetAnimation,this,[times,target,targetY,speed])
			 }
		}		
	}
}