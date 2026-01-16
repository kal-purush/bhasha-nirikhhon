/**
 * Created by yotam on 3/7/2015.
 */
import BaseService = require("../../components/arts/BaseService");
import IService = require('../../components/arts/interface/IService');

class TestService extends BaseService {
    static NAME:string = 'testSrv';
    constructor(private $q:any){
        super();
    }
    func (){
        var d = this.$q.defer();
        d.resolve();
        return d.promise;
    }
/*    restrict:string = 'E';
    template:string = '<md-toolbar><ng-transclude></ng-transclude></md-toolbar>';
    scope:{

    };
    controller:any = ToolbarController;
    controllerAs:string = 'vm';
    transclude:boolean = true;*/
}

export = TestService;