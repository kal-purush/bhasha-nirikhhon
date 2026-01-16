import './styles.scss';
import React, { Component } from 'react';
import Modal from 'react-modal';
import _ from 'lodash';

class ItemModal extends Component {
    render() {
        const itemData = _.get(this.props, 'currentItem.fields')
        return (
            <Modal
                isOpen={this.props.currentItem != null}
            >
                <img src={_.get(itemData, 'Images[0].thumbnails.large.url')}/>
                <p>{_.get(itemData, 'Name')}</p>
                <button onClick={this.props.clearItemView}>close</button>
            </Modal>
        );
    }
}

export default ItemModal;