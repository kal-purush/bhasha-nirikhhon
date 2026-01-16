export default class AppController {
  public static $inject = ['apiService', 'coAuthenticateService', '$modal', '$window']
  constructor (
    public apiService,
    public coAuthenticateService,
    private $modal,
    private $window
  ) { }

  apiUrl = this.$window.appConfigParams.apiUrl

  public switchEnv (options) {
    let modalTemplate = `
      <form ng-submit="vm.save()" name="addModalForm">
        <div class="modal-header">
          <h3>Switch Environment</h3>
        </div>
        <div class="modal-body">
          <div class="row">
            <div class="col-xs-12">
              <label>API URL</label>
              <input type="text" class="form-control"
                ng-init="vm.newApiUrl = vm.options.apiUrl"
                ng-model="vm.newApiUrl">
              <br>
              <label>Token</label>
              <input type="text" class="form-control"
                ng-init="vm.newAccessTokenId = vm.options.accessTokenId"
                ng-model="vm.newAccessTokenId">
            </div>
          </div>
        </div>
        <div class="modal-footer" co-submit-buttons="enabledPristine">
          <button type="button" class="btn btn-default" ng-click="vm.cancel()">Cancel</button>
          <button type="submit" class="btn btn-success">Go</button>
        </div>
      </form>
    `

    var ModalController = ['$scope', '$modalInstance', '$window', 'options',
      function ($scope, $modalInstance, $window, options) {
        $scope.vm = this
        this.options = options
        this.cancel = $modalInstance.dismiss
        this.save = () => {
          $window.localStorage['appConfigParams'] = JSON.stringify({
            apiUrl: $scope.vm.newApiUrl
          })
          $window.localStorage['$LoopBack$accessTokenId'] = $scope.vm.newAccessTokenId
          $window.location.reload()
        }
      }
    ]

    let modal = this.$modal.open({
      template: modalTemplate,
      controller: ModalController,
      controllerAs: 'vm',
      resolve: {
        options: ['$window', function($window) {
          return {
            apiUrl: $window.appConfigParams.apiUrl,
            accessTokenId: $window.localStorage['$LoopBack$accessTokenId']
          }
        }]
      }
    })
    modal.result.then((res) => {
      console.log(res)
    })
  }
}