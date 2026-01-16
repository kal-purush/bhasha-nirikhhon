console.log('hi there');
const buildApp = {
    title: 'Visibility Toggle',
}

class Visibility extends React.Component {
    constructor(props) {
        super(props);
        this.changeState = this.changeState.bind(this);
        this.state = {
            visibility: true
        }
    }

    changeState() {
        this.setState((prevState) => {
            return {
                visibility: !prevState.visibility
            }
        })
        
    }
    render() {
        return (
            <div>
            {buildApp.title && <h1>{buildApp.title}</h1>}
            <button onClick={this.changeState}>{this.state.visibility ? 'hide details' : 'show details'}</button>
            {this.state.visibility && (
                <div>
                <p>these are hidden details</p>
                </div>
            )}
            
        </div>
        )
    }
}
ReactDOM.render(<Visibility />, document.getElementById('app'));

