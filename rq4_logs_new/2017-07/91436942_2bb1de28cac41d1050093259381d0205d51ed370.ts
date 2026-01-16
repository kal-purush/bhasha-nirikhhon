import { tag, Tag } from '@storefront/core';
import RefinementControls from '../refinement-controls';

@tag('gb-range-refinement-controls', require('./index.html'))
class RangeRefinementControls extends RefinementControls<RangeRefinementControls.Props> {

  refs: {
    low: HTMLInputElement,
    high: HTMLInputElement
  };
  props: RangeRefinementControls.Props = {
    labels: {
      low: 'Min',
      high: 'Max',
      submit: 'Submit'
    }
  };

  init() {
    super.init();
    this.expose('rangeControls', this.props);
  }

  search = () => {
    const low = parseFloat(this.refs.low.value);
    const high = parseFloat(this.refs.high.value);
    this.actions.addRefinement(this.props.field, low, high);
  }
}

interface RangeRefinementControls extends RefinementControls<RangeRefinementControls.Props> { }
namespace RangeRefinementControls {
  export interface Props extends RefinementControls.Props {
    labels: {
      low: string;
      high: string;
      submit: string;
    };
  }
}

export default RangeRefinementControls;