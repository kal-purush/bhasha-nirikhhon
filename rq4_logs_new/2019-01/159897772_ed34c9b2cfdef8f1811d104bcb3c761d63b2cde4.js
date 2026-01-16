(function main(React, ReactNative, componentState, Button, StyleSheet1, 
    responsiveHeight, responsiveFontSize,Header,Footer,Platform,CustomDivider, 
    navigate,Loader,_nativebase,require) {
    'use strict';

  var titleSize = 3.2;
   var grayColor = "#d9d9d9";

  var styles = StyleSheet1.create({
    bottomLayer: {
        flexDirection: 'row',
        justifyContent: 'center',
        alignItems: 'center',
        marginLeft: responsiveHeight(2),
        marginRight: responsiveHeight(2),
    },
    container: {
        flex: 1,
    },
    padding: {
        padding: responsiveHeight(2),
    },
    scrollView: {
        flex: 1,
        backgroundColor: "white",
        padding: responsiveHeight(2)
    },
    headerContainter: {
        flexDirection: "row",
        paddingTop: responsiveHeight(2),
        paddingBottom: responsiveHeight(2),
        // alignContent:"center",
        backgroundColor: "blue",
        justifyContent: 'space-around',
        alignItems: "center"
    },
    accountSummaryContainer: {
        paddingBottom: responsiveHeight(2),
        paddingLeft: responsiveHeight(1.5)
    },
    accountSummaryText: {
        fontSize: responsiveFontSize(titleSize),
        textAlign: "justify"
    },
    customStyle: {
        backgroundColor: "green"
    },
    headerBorder: {
        alignItems: "center",
        flexDirection: "row",
        paddingTop: responsiveHeight(0.3)
    },
    mainContainer: {
        justifyContent: "flex-start", backgroundColor: "white", padding: responsiveHeight(1)
    },
    grayLine: {
        marginLeft: responsiveHeight(1), marginRight: responsiveHeight(1)
    }
});


    var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

    ///var _react = React;

    var _react2 = React; //_interopRequireDefault(_react);

    var _reactNative = ReactNative;

    //function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

    function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

    function _possibleConstructorReturn(self, call) { if (!self) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return call && (typeof call === "object" || typeof call === "function") ? call : self; }

    function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; }


    function showLoading(localState) {
        localState.setState({ isLoading: true });
    }

    function hideLoading(localState) {
        //console.warn("hide Loading");
        localState.setState({ isLoading: false });
    }
    
  function logout(localState){ 
      componentState.api.fetchAsync("https://cfsfiserv.com/QEUATSMT/api/Authentication/LogOut",
                            "POST",
                            {
                                "X-CSRF-TOKEN": localState.state.loginToken
                            }
                        ).then(function(result2)  {
                            hideLoading(localState);
                          
                            componentState.localStorage.deleteData("LoginData").then(function(result) {
                                navigate('Login');
                            });   
                            navigate("Login");
                        }).catch(function(e){
                          hideLoading(localState);
                          componentState.localStorage.deleteData("LoginData").then(function(result) {
                                navigate('Login');
                            });  
                        });
    }
    
    function changeUserIdPost(localState){

        console.warn(localState.state.newUserId);
        console.warn(localState.state.confirmUserId);

        if(!(localState.state.newUserId) || !(localState.state.confirmUserId)){
            _nativebase.Toast.show({
                text: "Please enter fields",
                position: 'bottom',
                buttonText: 'Okay',
                duration: 5000,
                type: 'danger'
            });
            return false
        }

        if((localState.state.newUserId) !== (localState.state.confirmUserId)){
            _nativebase.Toast.show({
                text: "Confirm User Id and new user id should be same.",
                position: 'bottom',
                buttonText: 'Okay',
                duration: 5000,
                type: 'danger'
            });
            return;
        }

        //showLoading(localState);

        componentState.localStorage.getData("Apis").then(function(result) {

            var jsonData = JSON.parse(result);
            var data = { "loginName": localState.state.newUserId };

            componentState.api.fetchAsync("http://192.168.1.15" + jsonData.profile.postLoginNameAt.url,
                    jsonData.profile.postLoginNameAt.method,
                    {
                        "X-CSRF-TOKEN": jsonData.sessionKey.antiForgeryToken,
                        "X-Request-Token": jsonData.profile.postLoginNameAt.token
                    },
                    JSON.stringify(data)
                ).then(function(result2){

                    if(result2.message){    
                        _nativebase.Toast.show({
                            text: result2.message,
                            position: 'bottom',
                            buttonText: 'Okay',
                            duration: 5000,
                            type: 'danger'
                        });
                    }else {
                        _nativebase.Toast.show({
                            text: "Error while changing user id",
                            position: 'bottom',
                            buttonText: 'Okay',
                            duration: 5000,
                            type: 'danger'
                        });
                    }

                    hideLoading(localState)

                }).catch(function(err){
                    alert(JSON.stringify(err));
                    hideLoading(localState)
                })
        })
    }

    var NewComponent = function (_React$Component) {
        _inherits(NewComponent, _React$Component);
        //console.log(_react2.Component)
        function NewComponent(props) {
            _classCallCheck(this, NewComponent);

            var _this = _possibleConstructorReturn(this, (NewComponent.__proto__ || Object.getPrototypeOf(NewComponent)).call(this, props));

            _this.state = {
                newUserId : "",
                existingUserId : "",
                confirmUserId: "",
                isLoading : false
            }
            return _this;
        }

        _createClass(NewComponent, [{
            key: 'componentDidMount',
            value: function componentDidMount() {

                var local = this;
                componentState.localStorage.getData("Apis").then(function(result) {
                    var jsonData = JSON.parse(result);
                    if(jsonData && jsonData.sessionKey && jsonData.sessionKey.username){
                        local.setState({ existingUserId : jsonData.sessionKey.username,loginToken:jsonData.sessionKey.antiForgeryToken })
                    }
                })

            }
        }, {
            key: 'render',
            value: function render() {
                var _this = this;
                var localObject = this;

                var i = 1;
                var isAndroid = Platform.OS == "android" ? true : false

                return _react2.createElement(_reactNative.View, { style: styles.container },
                    [

                        _react2.createElement(Loader, { key: ++i, isLoading: _this.state.isLoading }
                            ,[
                            ]),
                        _react2.createElement(_reactNative.View,
                            { key: ++i, style: isAndroid ? {} : { paddingTop: 10 } },
                            [
                                _react2.createElement(Header, { key: ++i })
                            ]),

                        _react2.createElement(_reactNative.View, { key: ++i, style: styles.mainContainer },
                            [
                                _react2.createElement(_reactNative.View, { key: ++i, style: styles.accountSummaryContainer },
                                    [
                                        _react2.createElement(_reactNative.Text, { key: ++i, style: styles.accountSummaryText }, ["Change User ID"])
                                    ]
                                ),
                                _react2.createElement(_reactNative.View, { key: ++i, style: styles.grayLine }, [
                                    _react2.createElement(CustomDivider, { key: ++i })
                                ])
                            ]),
                        _react2.createElement(_reactNative.ScrollView, { key: ++i, style: styles.scrollView },
                            [
                                // inside the ScrollView 
                                _react2.createElement(_reactNative.View, { key: ++i, style: { height: 500 } }, [

                                    _react2.createElement(_reactNative.View, { key: ++i, style: { flex: 1 } }, [
                                        _react2.createElement(_reactNative.View, { key: ++i }, [
                                            _react2.createElement(_reactNative.Text, { key: ++i }, [
                                                "Your user Id must be between 8 and 26 characters in length and may be made up of both letters and numerals. Your user ID is not case sensitive."
                                            ])
                                        ]),

                                        //
                                        _react2.createElement(_reactNative.View, { key: ++i }, [
                                            _react2.createElement(_reactNative.View, {
                                                key: ++i, style: {
                                                    marginTop: responsiveHeight(2),
                                                    backgroundColor: "#FAFAFA", borderRadius: 1, padding: responsiveHeight(2.2)
                                                }
                                            },
                                                [
                                                    _react2.createElement(_reactNative.View, { key: ++i, style: { marginTop: responsiveHeight(1), marginBottom: 0 } }, [
                                                        // 
                                                        _react2.createElement(_reactNative.View, { key: ++i }, [
                                                            _react2.createElement(_reactNative.Text, { key: ++i, style: { color: "gray" } }, [
                                                                "Existing User ID"
                                                            ])
                                                        ]),
                                                        _react2.createElement(_reactNative.View, { key: ++i }, [
                                                            //
                                                            _react2.createElement(_reactNative.Text, { key: ++i }, [
                                                                _this.state.existingUserId
                                                            ])
                                                        ])
                                                    ]),

                                                    _react2.createElement(_reactNative.View, { key: ++i, style: { marginTop: responsiveHeight(3) } }, [
                                                        // 
                                                        _react2.createElement(_reactNative.View, { key: ++i }, [
                                                            _react2.createElement(_reactNative.Text, { key: ++i, style: { color: "gray" } }, [
                                                                "New User ID"
                                                            ])
                                                        ]),
                                                        _react2.createElement(_reactNative.View, { key: ++i, style: { marginTop: responsiveHeight(1) } }, [
                                                            //
                                                            _react2.createElement(_reactNative.TextInput, {
                                                                key: ++i, style: { borderWidth: 0.9, padding: 0, backgroundColor: "white", borderColor: "gray" },
                                                                onChangeText: function (val) {
                                                                    _this.setState({ newUserId: val })
                                                                },
                                                            }, [
                                                                _this.state.newUserId
                                                                ])
                                                        ])
                                                    ])
                                                    ,
                                                    _react2.createElement(_reactNative.View, { key: ++i, style: { marginTop: responsiveHeight(3) } }, [
                                                        // 
                                                        _react2.createElement(_reactNative.View, { key: ++i }, [
                                                            _react2.createElement(_reactNative.Text, { key: ++i, style: { color: "gray" } }, [
                                                                "Confirm  User ID"
                                                            ])
                                                        ]),
                                                        _react2.createElement(_reactNative.View, { key: ++i, style: { marginTop: responsiveHeight(1) } }, [
                                                            //
                                                            _react2.createElement(_reactNative.TextInput, {
                                                                key: ++i, style: { borderWidth: 0.9, padding: 0, backgroundColor: "white", borderColor: "gray" }
                                                                ,onChangeText: function (val) {
                                                                    _this.setState({ confirmUserId: val })
                                                                },
                                                            },
                                                                [
                                                                    _this.state.confirmUserId
                                                                ])
                                                        ])
                                                    ])

                                                ]),
                                            _react2.createElement(_reactNative.View, { key: ++i, style: { marginTop: responsiveHeight(4) } }, [

                                                _react2.createElement(_reactNative.View, { key: ++i, style: { marginTop: responsiveHeight(2) } }, [
                                                    // 
                                                    _react2.createElement(_reactNative.View, { key: ++i }, [
                                                        _react2.createElement(Button, {
                                                            key: ++i, style: { fontSize: responsiveFontSize(2), fontWeight: "normal", padding: responsiveHeight(0.5), color: "white", borderWidth: responsiveHeight(0.1), borderColor: "#015EBF", backgroundColor: "#0061b8" },
                                                            onPress:  function () {
                                                                  changeUserIdPost(localObject);
                                                                           //  alert(_this.state.confirmUserId)
                                                            }
                                                        }, [
                                                                "Save Changes",

                                                            ])
                                                    ])
                                                ]),
                                                _react2.createElement(_reactNative.View, { key: ++i, style: { marginTop: responsiveHeight(1.9) } }, [
                                                    // 
                                                    _react2.createElement(_reactNative.View, { key: ++i }, [
                                                        _react2.createElement(Button, {
                                                            key: ++i, style: { fontSize: responsiveFontSize(2), fontWeight: "normal", padding: responsiveHeight(0.5), borderWidth: responsiveHeight(0.1), borderColor: "#0061b8", backgroundColor: "white" },
                                                            onPress:  function () {
                                                                alert(_this.state.confirmUserId)
                                                            }
                                                        }, [
                                                                "Cancel"
                                                            ])
                                                    ])
                                                ])
                                            ])
                                        ])
                                    ])
                                ]),
                            ])
                        ,
                        _react2.createElement(Footer, { key: ++i,

                            changeUser:function(){
                                navigate("ChangeUserId")
                            },
                            accSummary:function(){
                                navigate("AccountSummary")
                            },
                            color1 :"gray",color2:"gray",color3:"gray",color4:"gray",color5:"green",logout:function(){ logout(_this); }
                        }
                        )
                    ]
                )

            }
        }]);

        return NewComponent;
    }(_react2.Component);

    return NewComponent
})