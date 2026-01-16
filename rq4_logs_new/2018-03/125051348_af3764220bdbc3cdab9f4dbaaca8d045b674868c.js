import { Container } from "unstated";

export default class SelectionContainer extends Container {
  state = {
    code: null,
    codesComplete: {},
    zoom: 1.5
  };

  zoomIn = () => this.setState({ zoom: this.state.zoom + 0.1 });
  zoomOut = () => this.setState({ zoom: this.state.zoom - 0.1 });

  selectFlagByCode = code => {
    this.setState({ code });
  };

  matchCountry = code => {
    if (this.state.code === code) {
      this.state.codesComplete[code] = true;
      this.setState({ codesComplete: this.state.codesComplete });
    }
  };
}