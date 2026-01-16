import React from "react";
import {connect} from "react-redux";
import {Card, Modal} from "semantic-ui-react";
import ProfileInfo from "./ProfileInfo";


const NewUserModal = (props) => {
    return (
        <Modal inverted={true} open={props.isNew}
               onUnmount={() => {
               }}
               onClose={() => {
               }}>
            <div style={{margin: '1%'}}>
                <Card style={{width: '100%'}}>
                    <Card.Content>
                        <ProfileInfo/>
                    </Card.Content>
                </Card>
            </div>
        </Modal>
    )
};


const mapStateToProps = state => {
    return {
        auth: state.auth
    }
};

export default connect(mapStateToProps, {})(NewUserModal);
