import { connect } from 'react-redux';
import ShowPageGraph from './show_page_graph'

const msp = state => {
    debugger
    return {
        currentUser: state.entities.users[state.session.id],
        profile: state.entities.profile,
        price: state.entities.prices,
        graphPrices: state.entities.graphPrices
    }
}

export default connect(msp,null)(ShowPageGraph);