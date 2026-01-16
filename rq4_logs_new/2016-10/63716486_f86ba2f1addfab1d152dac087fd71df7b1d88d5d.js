var account=angular.module('accountModule');

account
.controller('accountCtrlStu',['$scope','accountSrvStu',function ($scope,accountSrv) {
	var self=$scope;
	self.profile='';

	self.flags={
		error:'',
		errorStatus:false,
		edit:false
	};
	self.enableEdit=function () {
		self.flags.edit=true;
	};
	self.saveProfile=function () {
		self.flags.edit=false;
		accountSrv.saveProfile(self.profile).then(function (res) {
			if(res.data.status){
				self.profile=res.data.message;
			}else{
				self.flags.errorStatus=false;
				self.flags.error=res.data.message;
			}
		},function (err) {
			self.flags.errorStatus=false;
			self.flags.error=err;
		});
	};
	accountSrv.getProfile().then(function (res) {
		if(res.data.status){
			self.profile=res.data.message;
		}else{
			self.flags.errorStatus=false;
			self.flags.error=res.data.message;
		}
	},function (err) {
		self.flags.errorStatus=false;
		self.flags.error=err;
	});/**/
}])
.service('accountSrvStu',function ($http) {
	var self=this;
	self.getProfile=function () {
		return $http.get('/profile');
	}
	self.saveProfile=function (data) {
		return $http.put('/profile',data);
	}
	self.updatePassword=function (data) {
		return $http.put('/signup',data);
	}

})
.controller('editPass',['$scope','accountSrvStu',function ($scope,accountSrv) {
	var self=$scope;
	self.password='';
	self.rePassword='';
	self.editPassword={
		submitted:false,
		hasError:false,
		checked:false,
		error:'',
		success:''
	};
	var reset=function () {
		self.password='';
		self.rePassword='';
	};

	self.submit=function () {
		self.editPassword.submitted=true;
		if(self.password!=self.rePassword){
			self.editPassword.hasError=true;
			self.editPassword.checked=true;
			self.editPassword.error='passwords must match, re enter the password';
			self.rePassword='';
		}else{
			accountSrv.updatePassword({password:self.password}).then(function (res) {
				if(res.data.status){
					self.editPassword.checked=true;
					self.editPassword.success='password updated successfully!';
				}else{
					self.editPassword.checked=true;
					self.editPassword.hasError=true;
					self.editPassword.error='There was a problem updating the password';
				}
			},function (err) {
				self.editPassword.checked=true;
				self.editPassword.hasError=true;
				self.editPassword.error=err;
			});
		}
	};
}]);