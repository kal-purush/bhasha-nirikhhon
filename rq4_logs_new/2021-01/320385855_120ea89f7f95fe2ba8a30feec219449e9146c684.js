import React, { Component } from 'react';
import { Modal, FormControl, Button } from 'react-bootstrap';
import PropTypes from 'prop-types';
import styles from './NewTask/newTaskStyle.module.css'



class EditTaskModal extends Component {
    constructor(props) {
        super(props);
        this.state = {
            ...props.data
        }
    }


    handleChange = (event) => {
        const { name, value } = event.target;

        this.setState({
            [name]: value
        })
    }

    handleKeyDown = (event) => {
        if (event.key === "Enter") {
            this.handleSubmit();
        }
    }

    handleSubmit = () => {
        const title = this.state.title.trim();
        const description = this.state.description.trim();


        if (!title) {
            return;
        }



        this.props.onSave({
            _id: this.state._id,
            title,
            description
        });

    }

    render() {

        const { onClose } = this.props;
        const { title, description } = this.state;

        return (
            <Modal
                show={true}
                onHide={onClose}
                size="lg"
                aria-labelledby="contained-modal-title-vcenter"
                centered
            >
                <Modal.Header closeButton>
                    <Modal.Title
                        id="contained-modal-title-vcenter"
                        className={styles._modalTitle}
                    >
                        Edit Task
                        </Modal.Title>
                </Modal.Header>
                <Modal.Body>
                    <FormControl
                        placeholder="Title"
                        name='title'
                        onChange={this.handleChange}
                        onKeyPress={this.handleKeyDown}
                        value={title}
                        className='mb-3'
                    />

                    <FormControl
                        placeholder="Description"
                        name='description'
                        as='textarea'
                        rows={3}
                        onChange={this.handleChange}
                        value={description}
                    />

                </Modal.Body>
                <Modal.Footer>
                    <Button
                        onClick={this.handleSubmit}
                        variant='success'
                    >
                        Save
                        </Button>
                    <Button
                        onClick={onClose}
                        variant='secondary'
                    >
                        Cancel</Button>
                </Modal.Footer>
            </Modal>
        )
    }




}

EditTaskModal.propTypes = {
    data: PropTypes.object.isRequired,
    onSave: PropTypes.func.isRequired,
    onClose: PropTypes.func.isRequired,
};

export default EditTaskModal;