(function(angular){
	//新建一个名叫serviceApp的模块 并创建服务Service
	angular.module('servicesApp',[]).service('Service',['$window',function ($window) {
		var str=$window.localStorage.getItem('myDatas')||'[]' //获取本地存储的数据 如果没有传入一个空数组
		var datas=JSON.parse(str)//将上面的数据转换成对象
		// 设置一个获取数据的方法 将数据返回到页面上
		this.get=function () {
			return datas;
		}
		//添加数据的功能
		this.add=function (newName) {
			datas.push({//把input的键入值添加到$scope.datas中
				id:Math.random(),
				name:newName,
				completed:false
			})
			console.log(datas);

			this.save()
		}
		//删除数据的功能
		this.remove=function (id) {
			for(var i=0;i<datas.length;i++){//遍历任务数据
				var data=datas[i]
				if(data.id==id){//当数据的id等于传入的id时 删除当前这条数据
					datas.splice(i,1);
					this.save()
					return;
				}
			}
		}
		//设置本地数据存储 把键入的数据放到本地存储中去
		this.save=function () {
			var str=JSON.stringify(datas)
			$window.localStorage.setItem('myDatas',str);
		}
		//点击右上角的全部选中的状态
		this.checkedAll=function (isChecked) {
			for(var i=0;i<datas.length;i++){
				datas[i].completed=isChecked
			}
		}
		//清除所有已完成的任务
		this.clear=function () {
			for(var i=datas.length-1;i>=0;i--){
				if(datas[i].completed){
					datas.splice(i,1)

				}
			}
			this.save()
		}
	}])

})(angular)
/*
* [//模拟表单数据
		{id:$scope.index++,name:"吃饭",completed:true},
			{id:$scope.index++,name:"吃饭",completed:true},
			{id:$scope.index++,name:"吃饭",completed:true},
			{id:$scope.index++,name:"吃饭",completed:true},
			{id:$scope.index++,name:"吃饭",completed:true}
		]*/