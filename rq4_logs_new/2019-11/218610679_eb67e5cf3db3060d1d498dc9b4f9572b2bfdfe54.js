import React,{Component} from 'react';
import './AddItemModal.css';

export default class OrderItemModal extends Component {
    
    state = {

    }
    componentDidMount(){
        console.log(this.props)
    }

    render(){
        return (
            <div>
            <div className="modal fade" id="AddItemModal">
                <div className="modal-dialog" role="document">
                    <div className="modal-content">
                        <div className="modal-header text-center">
                            <h4 className="modal-title w-100 font-weight-bold">Success!</h4>
                            <button type="button" className="close" data-dismiss="modal" aria-label="Close">
                                <span aria-hidden="true">&times;</span>
                            </button>
                        </div>
                        <div className="modal-body modal-container">
                            <h2 className="modal-item-title">Olaf<i class="fas fa-carrot"></i></h2>
                            <p className="order-details">Item(s) have been added to your cart</p>
                            <p className="order-details">You can proceed to your shopping cart <a href="/cart">HERE</a></p>
                        </div>
                    </div>
                </div>
            </div>
        </div>
        )
    }

}
