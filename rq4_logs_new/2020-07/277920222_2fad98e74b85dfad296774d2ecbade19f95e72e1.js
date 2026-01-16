var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

function _possibleConstructorReturn(self, call) { if (!self) { throw new ReferenceError("this hasn't been initialised - super() hasn't been called"); } return call && (typeof call === "object" || typeof call === "function") ? call : self; }

function _inherits(subClass, superClass) { if (typeof superClass !== "function" && superClass !== null) { throw new TypeError("Super expression must either be null or a function, not " + typeof superClass); } subClass.prototype = Object.create(superClass && superClass.prototype, { constructor: { value: subClass, enumerable: false, writable: true, configurable: true } }); if (superClass) Object.setPrototypeOf ? Object.setPrototypeOf(subClass, superClass) : subClass.__proto__ = superClass; }

var ReactSlider = function (_React$Component) {
    _inherits(ReactSlider, _React$Component);

    function ReactSlider(props) {
        _classCallCheck(this, ReactSlider);

        return _possibleConstructorReturn(this, (ReactSlider.__proto__ || Object.getPrototypeOf(ReactSlider)).call(this, props));
    }

    _createClass(ReactSlider, [{
        key: "render",
        value: function render() {

            return React.createElement(
                "div",
                { className: "slider" },
                React.createElement(
                    "div",
                    { className: "slider__inner" },
                    React.createElement(
                        "div",
                        { className: "slider__item" },
                        React.createElement(
                            "p",
                            { className: "slider__item-name" },
                            "\u0413\u043E\u043B\u0438\u043A\u043E\u0432\u0430 \u0422\u0430\u0442\u044C\u044F\u043D\u0430 \u0410\u043B\u0435\u043A\u0441\u0435\u0435\u0432\u043D\u0430"
                        ),
                        React.createElement(
                            "div",
                            { className: "slider__item-box" },
                            React.createElement(
                                "div",
                                { className: "slider__item-number" },
                                "7"
                            ),
                            React.createElement(
                                "div",
                                { className: "slider__item-imagebg" },
                                React.createElement("img", { className: "slider__item-image", src: "./images/user_golikova.png", alt: "" })
                            )
                        )
                    ),
                    React.createElement(
                        "div",
                        { className: "slider__item" },
                        React.createElement(
                            "p",
                            { className: "slider__item-name" },
                            "\u0425\u0443\u0441\u043D\u0443\u043B\u043B\u0438\u043D \u041C\u0430\u0440\u0430\u0442 \u0428\u0430\u043A\u0438\u0440\u0437\u044F\u043D\u043E\u0432\u0438\u0447"
                        ),
                        React.createElement(
                            "div",
                            { className: "slider__item-box" },
                            React.createElement(
                                "div",
                                { className: "slider__item-number" },
                                "6"
                            ),
                            React.createElement(
                                "div",
                                { className: "slider__item-imagebg" },
                                React.createElement("img", { className: "slider__item-image", src: "./images/user_husnullin.png", alt: "" })
                            )
                        )
                    ),
                    React.createElement(
                        "div",
                        { className: "slider__item" },
                        React.createElement(
                            "p",
                            { className: "slider__item-name" },
                            "\u0411\u0435\u043B\u043E\u0443\u0441\u043E\u0432 \u0410\u043D\u0434\u0440\u0435\u0439 \u0420\u044D\u043C\u043E\u0432\u0438\u0447"
                        ),
                        React.createElement(
                            "div",
                            { className: "slider__item-box" },
                            React.createElement(
                                "div",
                                { className: "slider__item-number" },
                                "5"
                            ),
                            React.createElement(
                                "div",
                                { className: "slider__item-imagebg" },
                                React.createElement("img", { className: "slider__item-image", src: "./images/user_belousov.png", alt: "" })
                            )
                        )
                    ),
                    React.createElement(
                        "div",
                        { className: "slider__item" },
                        React.createElement(
                            "p",
                            { className: "slider__item-name" },
                            "\u0413\u0440\u0438\u0433\u043E\u0440\u0435\u043D\u043A\u043E \u0414\u043C\u0438\u0442\u0440\u0438\u0439 \u042E\u0440\u044C\u0435\u0432\u0438\u0447"
                        ),
                        React.createElement(
                            "div",
                            { className: "slider__item-box" },
                            React.createElement(
                                "div",
                                { className: "slider__item-number" },
                                "1"
                            ),
                            React.createElement(
                                "div",
                                { className: "slider__item-imagebg" },
                                React.createElement("img", { className: "slider__item-image", src: "./images/user_grigorenko.png", alt: "" })
                            )
                        )
                    ),
                    React.createElement(
                        "div",
                        { className: "slider__item" },
                        React.createElement(
                            "p",
                            { className: "slider__item-name" },
                            "\u0411\u043E\u0440\u0438\u0441\u043E\u0432 \u042E\u0440\u0438\u0439 \u0418\u0432\u0430\u043D\u043E\u0432\u0438\u0447"
                        ),
                        React.createElement(
                            "div",
                            { className: "slider__item-box" },
                            React.createElement("div", { className: "slider__item-number ok-large" }),
                            React.createElement(
                                "div",
                                { className: "slider__item-imagebg" },
                                React.createElement("img", { className: "slider__item-image", src: "./images/user_borisov.png", alt: "" })
                            )
                        )
                    ),
                    React.createElement(
                        "div",
                        { className: "slider__item" },
                        React.createElement(
                            "p",
                            { className: "slider__item-name" },
                            "\u0410\u0431\u0440\u0430\u043C\u0447\u0435\u043D\u043A\u043E \u0412\u0438\u043A\u0442\u043E\u0440\u0438\u044F \u0412\u0430\u043B\u0435\u0440\u0438\u0435\u0432\u043D\u0430"
                        ),
                        React.createElement(
                            "div",
                            { className: "slider__item-box" },
                            React.createElement("div", { className: "slider__item-number ok-large" }),
                            React.createElement(
                                "div",
                                { className: "slider__item-imagebg" },
                                React.createElement("img", { className: "slider__item-image", src: "./images/user_abramchenko.png", alt: "" })
                            )
                        )
                    ),
                    React.createElement(
                        "div",
                        { className: "slider__item" },
                        React.createElement(
                            "p",
                            { className: "slider__item-name" },
                            "\u0410\u0431\u0440\u0430\u043C\u0447\u0435\u043D\u043A\u043E \u0412\u0438\u043A\u0442\u043E\u0440\u0438\u044F \u0412\u0430\u043B\u0435\u0440\u0438\u0435\u0432\u043D\u0430"
                        ),
                        React.createElement(
                            "div",
                            { className: "slider__item-box" },
                            React.createElement("div", { className: "slider__item-number ok-large" }),
                            React.createElement(
                                "div",
                                { className: "slider__item-imagebg" },
                                React.createElement("img", { className: "slider__item-image", src: "./images/user_abramchenko.png", alt: "" })
                            )
                        )
                    ),
                    React.createElement(
                        "div",
                        { className: "slider__item" },
                        React.createElement(
                            "p",
                            { className: "slider__item-name" },
                            "\u0411\u043E\u0440\u0438\u0441\u043E\u0432 \u042E\u0440\u0438\u0439 \u0418\u0432\u0430\u043D\u043E\u0432\u0438\u0447"
                        ),
                        React.createElement(
                            "div",
                            { className: "slider__item-box" },
                            React.createElement("div", { className: "slider__item-number ok-large" }),
                            React.createElement(
                                "div",
                                { className: "slider__item-imagebg" },
                                React.createElement("img", { className: "slider__item-image", src: "./images/user_borisov.png", alt: "" })
                            )
                        )
                    ),
                    React.createElement(
                        "div",
                        { className: "slider__item" },
                        React.createElement(
                            "p",
                            { className: "slider__item-name" },
                            "\u0413\u0440\u0438\u0433\u043E\u0440\u0435\u043D\u043A\u043E \u0414\u043C\u0438\u0442\u0440\u0438\u0439 \u042E\u0440\u044C\u0435\u0432\u0438\u0447"
                        ),
                        React.createElement(
                            "div",
                            { className: "slider__item-box" },
                            React.createElement(
                                "div",
                                { className: "slider__item-number" },
                                "1"
                            ),
                            React.createElement(
                                "div",
                                { className: "slider__item-imagebg" },
                                React.createElement("img", { className: "slider__item-image", src: "./images/user_grigorenko.png", alt: "" })
                            )
                        )
                    ),
                    React.createElement(
                        "div",
                        { className: "slider__item" },
                        React.createElement(
                            "p",
                            { className: "slider__item-name" },
                            "\u0411\u0435\u043B\u043E\u0443\u0441\u043E\u0432 \u0410\u043D\u0434\u0440\u0435\u0439 \u0420\u044D\u043C\u043E\u0432\u0438\u0447"
                        ),
                        React.createElement(
                            "div",
                            { className: "slider__item-box" },
                            React.createElement(
                                "div",
                                { className: "slider__item-number" },
                                "5"
                            ),
                            React.createElement(
                                "div",
                                { className: "slider__item-imagebg" },
                                React.createElement("img", { className: "slider__item-image", src: "./images/user_belousov.png", alt: "" })
                            )
                        )
                    ),
                    React.createElement(
                        "div",
                        { className: "slider__item" },
                        React.createElement(
                            "p",
                            { className: "slider__item-name" },
                            "\u0425\u0443\u0441\u043D\u0443\u043B\u043B\u0438\u043D \u041C\u0430\u0440\u0430\u0442 \u0428\u0430\u043A\u0438\u0440\u0437\u044F\u043D\u043E\u0432\u0438\u0447"
                        ),
                        React.createElement(
                            "div",
                            { className: "slider__item-box" },
                            React.createElement(
                                "div",
                                { className: "slider__item-number" },
                                "6"
                            ),
                            React.createElement(
                                "div",
                                { className: "slider__item-imagebg" },
                                React.createElement("img", { className: "slider__item-image", src: "./images/user_husnullin.png", alt: "" })
                            )
                        )
                    ),
                    React.createElement(
                        "div",
                        { className: "slider__item" },
                        React.createElement(
                            "p",
                            { className: "slider__item-name" },
                            "\u0413\u043E\u043B\u0438\u043A\u043E\u0432\u0430 \u0422\u0430\u0442\u044C\u044F\u043D\u0430 \u0410\u043B\u0435\u043A\u0441\u0435\u0435\u0432\u043D\u0430"
                        ),
                        React.createElement(
                            "div",
                            { className: "slider__item-box" },
                            React.createElement(
                                "div",
                                { className: "slider__item-number" },
                                "7"
                            ),
                            React.createElement(
                                "div",
                                { className: "slider__item-imagebg" },
                                React.createElement("img", { className: "slider__item-image", src: "./images/user_golikova.png", alt: "" })
                            )
                        )
                    )
                )
            );
        }
    }]);

    return ReactSlider;
}(React.Component);

var domContainer = document.querySelector('#react_slider');
ReactDOM.render(React.createElement(ReactSlider, null), domContainer);