/*modules*/
import React from 'react';
import { BrowserRouter as Router, Route, Switch, Redirect } from 'react-router-dom';
import { Spinner } from 'react-bootstrap';
/*js*/
import API from './js/API';
import { AppNavbar } from './js/Navbar';
import { CarsShow } from './js/Car';
import { OptionalErrorMsg } from './js/ErrorMsg';
import { Login } from './js/LoginComponent';
import { PastRentals } from './js/PastRentals';
import { FutureRentals } from './js/FutureRentals';
import { InteractiveConfiguration } from './js/InConfig'
/*css*/
import './App.css';
import 'bootstrap/dist/css/bootstrap.min.css';
class App extends React.Component {

  constructor(props) {

    super(props)
    this.state = {
      isLoggedIn: false, // User is authenticated
      errorMsg: '', // Error message received by an API call
      loginError: false,  // Need to display that login action failed
      user: '',  // Name of user to display when authenticated
      csrfToken: null,
      loading: false,
      cars: [], //Array of cars
      selectedBrands: [],
      selectedCategories: [],
    }
    this.cars = []; //All cars
    this.brands = [];//All brands
    this.categories = ['A', 'B', 'C', 'D', 'E'];
  }

  componentDidMount() {
    this.loadInitialData();
  }

  //Load all cars and brand
  loadInitialData() {

    this.setState({ loading: true }, () => {
      API.getAllCars().then((c) => {
        this.cars = [...c];
        this.setState({ cars: [...c] });
      }).catch(
        (errorObj) => {
          this.handleErrors(errorObj);
          this.setState({ loading: false });
        }
      );
      API.getAllBrands().then((b) => {
        this.brands = b.map(b => b.brand);
        this.setState({ loading: false, selectedBrands: this.brands, selectedCategories: ['A', 'B', 'C', 'D', 'E'] });
      }).catch(
        (errorObj) => {
          this.handleErrors(errorObj);
          this.setState({ loading: false });
        }
      )
    });

  }

  /*ERRORS*/
  handleErrors(errorObj) {
    if (errorObj) {
      if (errorObj.status && errorObj.status === 401) {
        //Redirect to /login
        setTimeout(() => { this.setState({ isLoggedIn: false, loginError: false, errorMsg: '' }) }, 2000);
      }
      const err0 = errorObj.errors[0];
      const errorString = err0.param + ': ' + err0.msg;
      this.setState({ errorMsg: errorString, });
    }
  }
  //Delete Error msg
  cancelErrorMsg = () => {
    this.setState({ errorMsg: '' });
  }

  /*LOGIN/LOGOUT*/
  userLogout = () => {
    this.setState({ loading: true });
    API.userLogout().then(
      () => { this.setState({ isLoggedIn: false, user: '', loading: false }) }
    );
  }
  setLoggedInUser = (name) => {
    this.setState({ isLoggedIn: true, user: name, loading: true });
    API.getCSRFToken().then((response) => this.setState({ csrfToken: response.csrfToken, loading: false }));
    this.loadInitialData();
  }

  /*Cars Filter*/
  handleBrandFilterChange = (selected) => {
    if (selected.length === 0) {
      selected = this.brands;
    }
    const tmp = this.cars.filter((c) => selected.includes(c.brand) && this.state.selectedCategories.includes(c.category));
    this.setState({ cars: [...tmp], selectedBrands: [...selected] });
  }

  handleCategoryFilterChange = (selected) => {
    if (selected.length === 0) {
      selected = this.categories;
    }
    const tmp = this.cars.filter((c) => selected.includes(c.category) && this.state.selectedBrands.includes(c.brand));
    this.setState({ cars: [...tmp], selectedCategories: [...selected] });
  }


  render() {
    return <Router>
      <Switch>
        <Route exact path='/login' render={(props) => {
          //If logged in redirect to '/'
          if (this.state.isLoggedIn)
            return <Redirect to='/user' />
          else
            return <>
              <AppNavbar isLoggedIn={this.state.isLoggedIn} userLogout={this.userLogout}></AppNavbar>
              <OptionalErrorMsg errorMsg={this.state.errorMsg} cancelErrorMsg={this.cancelErrorMsg} />
              <Login setLoggedInUser={this.setLoggedInUser} />
            </>
        }}>
        </Route>
        <Route exact path='/' render={(props) => {
          if (this.state.loading) {
            return <>
              <AppNavbar isLoggedIn={this.state.isLoggedIn} userLogout={this.userLogout}></AppNavbar>
              <Spinner animation="border" variant="dark" />
            </>
          } else {

            let isLoggedIn = this.state.isLoggedIn;
            if (props.location.state && props.location.state.isLoggedIn)
              isLoggedIn = props.location.state.isLoggedIn;
            if (isLoggedIn)
              return <Redirect to='/user' />
            else {
              return <>
                <AppNavbar isLoggedIn={this.state.isLoggedIn} userLogout={this.userLogout}></AppNavbar>
                <OptionalErrorMsg errorMsg={this.state.errorMsg} cancelErrorMsg={this.cancelErrorMsg} />
                <CarsShow cars={this.state.cars} brands={this.brands} brandFilter={this.handleBrandFilterChange} categoryFilter={this.handleCategoryFilterChange}></CarsShow>
              </>
            }
          }
        }}
        ></Route>
        <Route exact path='/user' render={(props) => {
          if (!this.state.isLoggedIn)
            return <Redirect to='/' />
          else {
            if (this.state.loading) {
              return <>
                <AppNavbar isLoggedIn={this.state.isLoggedIn} userLogout={this.userLogout}></AppNavbar>
                <Spinner animation="border" variant="dark" />
              </>
            } else {
              return <>
                <AppNavbar isLoggedIn={this.state.isLoggedIn} userLogout={this.userLogout}></AppNavbar>
                <OptionalErrorMsg errorMsg={this.state.errorMsg} cancelErrorMsg={this.cancelErrorMsg} />
                <InteractiveConfiguration csrfToken={this.state.csrfToken} user={this.state.user} handleErrors={(errorObj) => this.handleErrors(errorObj)} ></InteractiveConfiguration>
              </>
            }
          }
        }}></Route>
        <Route exact path='/myPastRentals' render={(props) => {
          if (!this.state.isLoggedIn)
            return <Redirect to='/' />
          else {
            if (this.state.loading) {
              return <>
                <AppNavbar isLoggedIn={this.state.isLoggedIn}></AppNavbar>
                <Spinner animation="border" variant="dark" />
              </>
            } else {
              return <>
                <AppNavbar isLoggedIn={this.state.isLoggedIn} userLogout={this.userLogout}></AppNavbar>
                <OptionalErrorMsg errorMsg={this.state.errorMsg} cancelErrorMsg={this.cancelErrorMsg} />
                <PastRentals handleErrors={(errorObj) => this.handleErrors(errorObj)} csrfToken={this.state.csrfToken}></PastRentals>
              </>
            }
          }
        }}>
        </Route>
        <Route exact path='/myFutureRentals' render={(props) => {
          if (!this.state.isLoggedIn)
            return <Redirect to='/' />
          else {
            if (this.state.loading) {
              return <>
                <AppNavbar isLoggedIn={this.state.isLoggedIn} userLogout={this.userLogout}></AppNavbar>
                <Spinner animation="border" variant="dark" />
              </>
            } else {
              return <>
                <AppNavbar isLoggedIn={this.state.isLoggedIn} userLogout={this.userLogout}></AppNavbar>
                <OptionalErrorMsg errorMsg={this.state.errorMsg} cancelErrorMsg={this.cancelErrorMsg} />
                <FutureRentals handleErrors={(errorObj) => this.handleErrors(errorObj)} csrfToken={this.state.csrfToken}></FutureRentals>
              </>
            }
          }
        }}>
        </Route>
        <Route render={(props) => {
            return <Redirect to='/' />
        }}>
        </Route>
      </Switch>
    </Router >
  }


}

export default App;