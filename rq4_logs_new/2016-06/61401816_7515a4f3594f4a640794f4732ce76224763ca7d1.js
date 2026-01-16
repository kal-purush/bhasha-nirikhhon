/**
 * @jsx React.DOM
 */

var BoxCollection = React.createClass({
    getInitialState: function() {
        var self = this;

        return {
            items: []
        }
    },
    render: function() {
        var self = this;

        return (
            <div>
                {self.state.items.map(function(item) {
                    return (
                        <BoxItem key={item.Id} Item={item.Name} />
                    )
                })}
            </div>
        )
    }
});
var BoxItem = React.createClass({
    render: function() {
        var self = this;

        return (
            <p>{self.props.Item}</p>
        )
    }
});