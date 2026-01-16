import React, {useEffect} from 'react'
import {useDispatch, useSelector} from 'react-redux'
import {Link} from 'react-router-dom'
import {deleteOrder, listOrders} from '../actions/orderActions'

const OrdersPage = (props) => {
  const orderList = useSelector(state => state.orderList)
  const {loading, orders, error} = orderList;
  const orderDelete = useSelector(state => state.orderDelete);
  const {loading: loadingDelete, success: successDelete, error: errorDelete} = orderDelete;
  const dispatch = useDispatch();

  const deleteHandler = order => {
    dispatch(deleteOrder(order.id));
  }

  useEffect(() => {
    dispatch(listOrders());
  }, [dispatch, orderDelete])

  return (
    loading ? <div>Loading...</div> :
      <div className="content content-margined">
        <div className="product-header">
          <h3>
            Orders
          </h3>
          <div className="order-list">
            <table className="table">
              <thead>
              <tr>
                <th>ID</th>
                <th>Date</th>
                <th>Price</th>
                <th>User</th>
                <th>Paid</th>
                <th>Paid At</th>
                <th>Delivered</th>
                <th>Delivered At</th>
                <th>Actions</th>
              </tr>
              </thead>
              <tbody>
              {orders.map(order =>
                <tr key={order.id}>
                  <th>{order.id}</th>
                  <th>{order.createdAt}</th>
                  <th>${order.totalPrice}</th>
                  <th>{order.user.name}</th>
                  <th>{order.isPaid ? 'Yes' : 'No'}</th>
                  <th>{order.paidAt}</th>
                  <th>{order.isDelivered ? 'Yes' : 'No'}</th>
                  <th>{order.deliveredAt}</th>
                  <th>
                    <Link to={`/order/${order.id}`} classname="button secondary">Details</Link>
                    {' '}
                  </th>
                  <th>
                    <button type="button" className="button secondary" onClick={() => deleteHandler(order)}>
                      Delete
                    </button>
                  </th>
                </tr>
              )}
              </tbody>
            </table>
          </div>
        </div>
      </div>
  )
}

export default OrdersPage;