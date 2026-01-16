import React, {useState} from 'react';
import Dialog from "@material-ui/core/Dialog";
import DialogTitle from "@material-ui/core/DialogTitle";
import DialogContent from "@material-ui/core/DialogContent";
import DialogContentText from "@material-ui/core/DialogContentText";
import DialogActions from "@material-ui/core/DialogActions";
import Button from "@material-ui/core/Button";
import TextField from "@material-ui/core/TextField";

export default function PromptDialog(props) {
    const [open, setOpen] = useState(true);
    const [userInput, setUserInput] = useState('');
    const [isInputValid, setIsInputValid] = useState(false);

    const handleGeneralClose = () => {
        setOpen(false);
        setTimeout(() => {
            props.dispatch({
                type: "CLOSE_ALERT",
                payload: {
                    id: props.id,
                }
            });
        }, 200);
    }

    const handleClose = () => {
        handleGeneralClose();
        props.callbackOnCancel();
    };

    const handleOk = () => {
        handleGeneralClose();
        props.callbackOnOk(userInput);
    };

    const handleChange = (input) => {
        if (props.callbackValidator(input)) {
            setIsInputValid(true);
        } else {
            setIsInputValid(false);
        }
        setUserInput(input);
    }

    return (
        <div>
            <Dialog
                open={open}
                onClose={handleClose}
                aria-labelledby="alert-dialog-title"
                aria-describedby="alert-dialog-description"
            >
                <DialogTitle id="alert-dialog-title">{props.title}</DialogTitle>
                <DialogContent>
                    <DialogContentText id="alert-dialog-description">
                        {props.message}
                    </DialogContentText>
                    <TextField value={userInput} autoFocus margin="dense" label={props.label} fullWidth
                        onChange={(e) => handleChange(e.target.value)}
                    />
                    <DialogContentText style={ {color: "crimson", visibility: !isInputValid ? "visible" : "hidden"}}>
                        <small>{props.validationMessage}</small>
                    </DialogContentText>
                </DialogContent>
                <DialogActions>
                    {props.showCancel ? <Button onClick={handleClose}>Anuluj</Button> : ''}
                    <Button onClick={handleOk} disabled={!isInputValid} color="primary" autoFocus>OK</Button>
                </DialogActions>
            </Dialog>
        </div>
    );
}