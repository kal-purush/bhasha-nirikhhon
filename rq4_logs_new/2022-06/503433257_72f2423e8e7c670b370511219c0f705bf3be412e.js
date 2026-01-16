import { Route, Switch, Redirect } from 'react-router-dom';

import Layout from './components/layout/Layout';
import QuoteDetail from './pages/QuoteDetail';
import AllQuotes from './pages/AllQuotes';
import NewQuote from './pages/NewQuote';
import NotFound from './pages/NotFound';

function App() { 
	return (
		<div>
			<Layout>
				<Switch>
					<Route path='/' exact>
						<Redirect to='/quotes' />
					</Route>
					<Route path='/quotes' exact>
						<AllQuotes />
					</Route>
					<Route path='/new-quotes'>
						<NewQuote />
					</Route>
					<Route path='/quotes/:quoteId'>
						<QuoteDetail />
					</Route>
					<Route path='*'>
						<NotFound />
					</Route>
				</Switch>
			</Layout>
		</div>
	);
}

export default App;