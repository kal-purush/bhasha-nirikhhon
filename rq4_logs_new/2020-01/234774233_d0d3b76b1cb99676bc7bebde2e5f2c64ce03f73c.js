import {Button, Icon, Modal, Header} from "semantic-ui-react";
import React from "react";

const CancelEvent = () => (
    <Modal trigger={""} basic size='small'>
        <Header icon='archive' content='Cancel Event' />
        <Modal.Content>
            <p>
                Are you sure you want to cancel the event?
            </p>
        </Modal.Content>
        <Modal.Actions>
            <Button basic color='red' inverted >
                <Icon name='remove'/> No
            </Button>
            <Button color='green' inverted >
                <Icon name='checkmark'/> Yes
            </Button>
        </Modal.Actions>
    </Modal>
);
export default CancelEvent;