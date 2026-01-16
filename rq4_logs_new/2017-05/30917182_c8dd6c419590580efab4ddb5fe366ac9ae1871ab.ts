/*
 * Copyright (c) 2017 The Finnish Board of Education - Opetushallitus
 *
 * This program is free software: Licensed under the EUPL, Version 1.1 or - as
 * soon as they will be approved by the European Commission - subsequent versions
 * of the EUPL (the "Licence");
 *
 * You may not use this work except in compliance with the Licence.
 * You may obtain a copy of the Licence at: http://ec.europa.eu/idabc/eupl
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * European Union Public Licence for more details.
 */

angular.module("app")
.config(($stateProvider) => {
    $stateProvider
        .state("root.selaus.perusteinfo", {
            url: "/perusteinfo/:perusteId",
            template: "<div ui-view></div>",
            resolve: {
                peruste: (PerusteApi, $stateParams) => PerusteApi.one("perusteet", $stateParams.perusteId).get(),
                opetussuunnitelmat(peruste) {
                    // TODO: Lataa paikalliset amosaasta perusteen pohjalta
                    return [{
                        nimi: { fi: "Hei moi" },
                        koulutustoimija: {
                            nimi: { fi: "Olen testiteksti" },
                        },
                        tutkintonimikkeet: ["tutkintonimikkeet_10092"],
                    }, {
                        nimi: { fi: "Joopa joo" },
                        koulutustoimija: {
                            nimi: { fi: "Lisää tekstiä ilman tutkintonimikettä" },
                        },
                        tutkintonimikkeet: [],
                    }];
                }
            },
            views: {
                "": {
                    templateUrl: "views/states/koostenakyma/peruste/view.html",
                    controller: ($scope, peruste) => {
                        $scope.peruste = peruste;
                    },
                },
                "paikalliset@root.selaus.perusteinfo": {
                    templateUrl: "views/states/koostenakyma/peruste/paikalliset.html",
                    controller($scope, peruste, opetussuunnitelmat) {
                        $scope.tutkintonimiketaulu = _.groupBy(peruste.tutkintonimikkeet, "tutkintonimikeUri");
                        console.log($scope.tutkintonimiketaulu);
                        $scope.opetussuunnitelmat = opetussuunnitelmat;
                    }
                },
            },
        });
});