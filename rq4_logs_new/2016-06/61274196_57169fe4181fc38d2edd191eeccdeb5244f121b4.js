//自定义组件 自定标签
var Message = React.createClass({
    render: function () {
        return <div>
            消息框
        </div>
    }
});
ReactDOM.render
(<Message></Message>,
    document.querySelector('#app')
);