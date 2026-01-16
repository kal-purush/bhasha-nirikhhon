/** @jsx React.DOM */
var Rating = React.createClass({

    getInitialState: function () {
        return {rating: 0, data: []};
    },
    handleGetScore: function (object) {
        this.createData(object);
    },
    createData: function (object) {
        var self = this;
        var data = [];
        for (var i = 0; i < 5; i++) {
            var checked = object.value > i ? 'checked' : "";
            var starData = {value: i, checked: checked};
            data.push(starData);
        }

        this.setState({data: data});
    },
    componentDidMount: function () {
        var self = this;
        $.getJSON(
            this.props.link,
            function (result) {
                self.handleGetScore(result);
            });
    },
    handleChange: function (index) {
        var self = this;
        $.getJSON(
            this.props.set,
            {data: index},
            function (result) {
                self.handleGetScore(result);
            });
    },
    render: function () {
        var self = this;
        return (<div className={'rating-area'}>
                    {self.state.data.map(function (rating, index) {
                        var star = rating.checked ? '★' : '☆';
                        return (<label className={'star-label'} for={'star-id-'.index}> {star}
                            <input id={'star-id-'.index} className={'stars-check'} checked={rating.checked} onClick={ function () {
                                self.handleChange(index)
                            } }  type="checkbox" value={rating.value}/>
                        </label>);
                    }) }
        </div>
        );
    }
});

React.render(
    <Rating link={handleGetNamesLink} set={handleChangeRating} />,
    document.getElementById('component-container')
);