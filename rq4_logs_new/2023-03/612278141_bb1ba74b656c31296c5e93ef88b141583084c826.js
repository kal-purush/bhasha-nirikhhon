import "../Videos/style/About.css";

class About extends React.Component {
    constructor() {
      super();
      this.state = {
        click: false,
      };
    }
  
    removeButtonClick = () => {
      this.setState({ click: !this.state.click });
    };
  
    render() {
      return (
        <section id="about">
          <div>
            {this.state.click ? (
              <Bios />
            ) : (
              <Team removeButtonClick={this.removeButtonClick} />
            )}
          </div>
        </section>
      );
    }
  }
  
  export default About;