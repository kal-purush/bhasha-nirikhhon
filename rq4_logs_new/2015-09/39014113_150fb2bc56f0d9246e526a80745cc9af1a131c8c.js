import {Test, findDOMNode, Component} from './helper';

class Bar extends Component {
    render() {
        return <div>{this.props.children}</div>
    }
}

describe("Refs", () => {
    it('components refs', () => {
        class Foo extends Component {
            componentDidMount() {
                expect(this.refs.bar1).toBeDefined();
                expect(this.refs.bar1 instanceof Bar).toBeTruthy();
                expect(this.refs.bar2).toBeUndefined();
            }

            componentDidUpdate() {
                //expect(this.refs.bar1).toBeUndefined();
                expect(this.refs.bar2).toBeDefined();
                expect(this.refs.bar2 instanceof Bar).toBeTruthy();
            }

            render() {
                return <Bar ref={this.props.refName}/>;
            }
        }
        new Test()
            .create(<Foo refName="bar1"/>)
            .update(<Foo refName="bar2"/>)
    });
    it('components subrefs', () => {
        class Foo extends Component {
            componentDidMount() {
                expect(this.refs.bar1).toBeDefined();
                expect(findDOMNode(this.refs.bar1) instanceof Node).toBeTruthy();
                expect(this.refs.bar2).toBeUndefined();
            }

            componentDidUpdate() {
                //expect(this.refs.bar1).toBeUndefined();
                expect(this.refs.bar2).toBeDefined();
                expect(findDOMNode(this.refs.bar2) instanceof Node).toBeTruthy();
            }

            render() {
                return <Bar><div ref={this.props.refName}>hello</div></Bar>;
            }
        }
        new Test()
            .create(<Foo refName="bar1"/>)
            .update(<Foo refName="bar2"/>)
    });
});