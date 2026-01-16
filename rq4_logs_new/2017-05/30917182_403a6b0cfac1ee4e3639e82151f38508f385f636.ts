angular.module('app')
.config(($stateProvider) => $stateProvider
.state('root.aipe.vaihe.oppiaine', {
    url: '/oppiaineet/:oppiaineId',
    templateUrl: 'eperusteet-esitys/views/oppiaine.html',
    controller: (vaihe, $scope, MurupolkuData, oppiaine) => {
        $scope.oppiaine = oppiaine;
        MurupolkuData.set({ oppiaineId: $scope.oppiaine.id, oppiaineNimi: $scope.oppiaine.nimi });
    },
    resolve: {
        oppiaineId: $stateParams => $stateParams.oppiaineId,
        oppiaine: (oppiaineId, perusteId, vaiheId, AipeOppiaineet) => AipeOppiaineet.get({
            perusteId: perusteId,
            vaiheId: vaiheId,
            oppiaineId: oppiaineId
        }).$promise
    }
}));