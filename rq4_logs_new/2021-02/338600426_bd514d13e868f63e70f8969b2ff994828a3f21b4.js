import React, { Component, Fragment } from 'react';
import styles from './BurgerBuilder.module.css';
import Burger from '../../components/Burger/Burger';
import BuilControls from '../../components/Burger/BuildControls/BuildControls';
import Modal from '../../components/UI/Modal/Modal';
import OrderSummary from '../../components/Burger/OrderSummary/OrderSummary';
import Spinner from '../../components/UI/Spinner/Spinner'
import axios from '../../axios-orders';
import errorComponent from '../ErrorComponent/ErrorComponent';

const INGREDIENT_PRICE = {
    salad: 20,
    bacon: 60,
    meat: 80,
    cheese: 40
}

class BurgerBuilder extends Component {

    state = {
        ingredient: {
            salad: 0,
            bacon: 0,
            meat: 0,
            cheese: 0
        },
        totalPrice: 100,
        purchasable: false,
        isOrdered: false,
        loading: false
    }

    updatePurchasable(ingredients) {
        const sum = Object.keys(ingredients).map(igKey => {
            return ingredients[igKey]
        }).reduce((arr, el) => {
            return arr + el
        }, 0);

        this.setState({ purchasable: sum > 0 })
    }

    addedIngredients = (type) => {
        const updatedCount = this.state.ingredient[type] + 1;
        const updatedIngredient = {
            ...this.state.ingredient
        }
        updatedIngredient[type] = updatedCount;

        const updatedPrice = this.state.totalPrice + INGREDIENT_PRICE[type]
        this.setState({
            ingredient: updatedIngredient,
            totalPrice: updatedPrice
        })
        this.updatePurchasable(updatedIngredient);
    }

    removeIngredients = (type) => {
        if (this.state.ingredient[type] !== 0) {
            const updatedCount = this.state.ingredient[type] - 1;
            const updatedIngredient = {
                ...this.state.ingredient
            }
            updatedIngredient[type] = updatedCount;
            const updatedPrice = this.state.totalPrice - INGREDIENT_PRICE[type]
            this.setState({
                ingredient: updatedIngredient,
                totalPrice: updatedPrice
            })
            this.updatePurchasable(updatedIngredient);
        } else {
            return;
        }
    }

    orderHandler = () => {
        this.setState({ isOrdered: true });
    }

    orderDisable = () => {
        this.setState({ isOrdered: false });
    }

    orderContinue = () => {
        this.setState({ loading : true})
        const orders = {
            ingredients: this.state.ingredient,
            price: this.state.totalPrice,
            customer: {
                name: 'Neymar',
                address: {
                    street: 'TowerStreet',
                    zipcode: 432789,
                    Country: 'france'
                }
            },
            deliveryMethod: 'fastest'
        }

        axios.post('/orders', orders).then(res => {
            this.setState({loading : false, isOrdered : false})
        }).catch(err => {
            this.setState({loading : false, isOrdered : false})
            console.log(err)

        });    
    }

    render() {
        let orderModal;
        const disabledInfo = {
            ...this.state.ingredient
        }

        for (let key in disabledInfo) {
            disabledInfo[key] = disabledInfo[key] <= 0
        }

        if (!this.state.loading) {
            orderModal = (
                <OrderSummary ingredients={this.state.ingredient}
                    orderDisable={this.orderDisable}
                    orderContinue={this.orderContinue}
                    totalPrice={this.state.totalPrice} /> 
            )
        } else {
            orderModal = <Spinner />
        }


        return (
            <Fragment>
                <Modal close={this.orderDisable} Show={this.state.isOrdered}>
                    {orderModal}
                </Modal>
                <div className={styles.mainArea}>
                    <Burger ingredients={this.state.ingredient} />
                    <BuilControls
                        controls={this.state.ingredient}
                        ingredientAdded={this.addedIngredients}
                        ingredientRemoved={this.removeIngredients}
                        disabled={disabledInfo}
                        price={this.state.totalPrice}
                        isPurchase={this.state.purchasable}
                        orderNow={this.orderHandler} />
                </div>
            </Fragment>
        );
    }
}

export default errorComponent(BurgerBuilder, axios);