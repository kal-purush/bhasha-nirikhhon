TestHelpers.InitalizeTestContext();

describe('Facebook.FacebookClient.getClientCounterList', () => {
    it("Should hit correct url", () => {
        TestHelpers.RequestUriTest($, "/management/social/facebookdisplaytypes");
        var testClient = new Xomni.Management.Social.Facebook.FacebookClient();
        testClient.getFacebookDisplayTypes(suc => { }, err => { });
    });

    it("Should use correct http method", () => {
        TestHelpers.RequestHttpMethodTest($, "Get");
        var testClient = new Xomni.Management.Social.Facebook.FacebookClient();
        testClient.getFacebookDisplayTypes(suc => { }, err => { });

    });

    it("Should use correct http headers", () => {
        TestHelpers.RequestHttpHeadersTest($);
        var testClient = new Xomni.Management.Social.Facebook.FacebookClient();
        testClient.getFacebookDisplayTypes(suc => { }, err => { });
    });

    it("Should parse response successfully", () => {
        TestHelpers.ResponseParseTest($, { '0': 'Page', '1': 'Popup', '2': 'Touch' });

        var expectedSuccess = (types: string)=> {
            expect(types["0"]).toEqual('Page');
            expect(types["1"]).toEqual('Popup');
            expect(types["2"]).toEqual('Touch');
        };

        var testClient = new Xomni.Management.Social.Facebook.FacebookClient();
        testClient.getFacebookDisplayTypes(expectedSuccess, err => { });
    });
});