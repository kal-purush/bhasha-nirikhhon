angular.module("app")
.service("Dokumentit", ($timeout, $stateParams) => {
    function dokumenttiUrlLataaja(Api, id) {
        return async function(scope, target = "dokumenttiUrl") {
            scope.$$ladataanDokumenttia = true;
            try {
                const doc = await Api.all("dokumentit").customGET("peruste", {
                    perusteId: id,
                    kieli: $stateParams.lang,
                    suoritustapa: $stateParams.suoritustapa
                });

                if (doc && doc.toString().length > 0) {
                    $timeout(() => {
                        scope[target] = location.origin + "/eperusteet-service/api/dokumentit/" + doc;
                        scope.$$ladataanDokumenttia = false;
                    });
                }
            }
            catch (ex) {
                scope.$$ladataanDokumenttia = false;
            }
        };
    }

    return {
        dokumenttiUrlLataaja
    };
});