Enact.react_class.Fields.Code = React.createClass({displayName: "Code",

    mixins: [Enact.react_class.Fields.BaseField],

    componentDidMount: function(){
        this.TextLiner = $(this.refs['raw'].getDOMNode()).TextLiner();
    },//componentDidMount

    onChange: function(event){
        this.props.field.value = event.target.value;
    },//onChange

    render: function(){
        return (
            React.createElement("div", null, 
                React.createElement("div", {ref: "raw", className: "text-liner"}, 
                    React.createElement("div", {className: "text-liner-numbers"}), 
                    React.createElement("div", {className: "text-liner-textarea"}, 
                        React.createElement("textarea", {ref: "raw-text", defaultValue: this.props.field.value, onChange: this.onChange})
                    )
                ), 

                React.createElement("div", {className: this.getErrorClass()}, "You must enter some code in this code area")

            )
        );
    }//render

});