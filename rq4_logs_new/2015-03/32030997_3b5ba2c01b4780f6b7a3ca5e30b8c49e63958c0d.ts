TestHelpers.InitalizeTestContext();

var samplePutRequest: Array<Xomni.Management.Configuration.TrendingActionTypes.TrendingActionTypeContainer> = [{ Id: 1, Description: "SocialLike", ImpactValue: 5.1 }];

describe('TrendingActionTypesClient.updateTrendingActionTypeValues', () => {

    it("Should hit correct url", () => {
        TestHelpers.RequestUriTest($, "/management/configuration/tenantactiontypes");
        var testClient = new Xomni.Management.Configuration.TrendingActionTypes.TrendingActionTypesClient;
        testClient.updateTrendingActionTypeValues(samplePutRequest, suc => { }, err => { });
    })


    it("Should use correct http method", () => {
        TestHelpers.RequestHttpMethodTest($, "Put");
        var testClient = new Xomni.Management.Configuration.TrendingActionTypes.TrendingActionTypesClient();
        testClient.updateTrendingActionTypeValues(samplePutRequest,suc => { }, err => { });

    });

    it("Should use correct http headers", () => {
        TestHelpers.RequestHttpHeadersTest($);

        var testClient = new Xomni.Management.Configuration.TrendingActionTypes.TrendingActionTypesClient();
        testClient.updateTrendingActionTypeValues(samplePutRequest,suc => { }, err => { });
    });

    it("Should parse response successfully", () => {
        TestHelpers.ResponseParseTest($,
            [
                {
                    "Id": 1,
                    "Description": "SocialLike",
                    "ImpactValue": 5.1
                }
            ]
        );

        var expectedSuccess = (trendingActionTypes: Xomni.Management.Configuration.TrendingActionTypes.TrendingActionTypeContainer[]) => {
            expect(trendingActionTypes[0].Id).toEqual(1);
            expect(trendingActionTypes[0].Description).toEqual("SocialLike");
            expect(trendingActionTypes[0].ImpactValue).toEqual(5.1);
        };

        var testClient = new Xomni.Management.Configuration.TrendingActionTypes.TrendingActionTypesClient();
        testClient.updateTrendingActionTypeValues(samplePutRequest, expectedSuccess, err=> { });
    });

    it("Should parse request successfully", () => {
        TestHelpers.RequestParseTest($, [{ 'Id': 1, 'Description': 'SocialLike', 'ImpactValue': 5.1 }]);

        var testClient = new Xomni.Management.Configuration.TrendingActionTypes.TrendingActionTypesClient();
        testClient.updateTrendingActionTypeValues(samplePutRequest, suc=> { }, err=> { });
    });
});