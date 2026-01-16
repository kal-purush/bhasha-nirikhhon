import Card from "@mui/material/Card";
import CardContent from "@mui/material/CardContent";
import Typography from "@mui/material/Typography";
import Grid from "@mui/material/Grid";
import CheckIcon from "@mui/icons-material/Check";
import IconButton from "@mui/material/IconButton";
import DeleteOutlineOutlinedIcon from "@mui/icons-material/DeleteOutlineOutlined";
import ModeEditOutlineOutlinedIcon from "@mui/icons-material/ModeEditOutlineOutlined";

// Components Imports
import { useContext, useState } from "react";
import { TodosContext } from "../contexts/todosContext";

//  Dialog Imports
import Button from "@mui/material/Button";
import Dialog from "@mui/material/Dialog";
import DialogActions from "@mui/material/DialogActions";
import DialogContent from "@mui/material/DialogContent";
import DialogContentText from "@mui/material/DialogContentText";
import DialogTitle from "@mui/material/DialogTitle";
import TextField from "@mui/material/TextField";

export default function Todo({ todo }) {
  const { todos, setTodos } = useContext(TodosContext);
  const [showDeleteDialog, setShowDeleteDialog] = useState(false);
  const [showUpdateDialog, setShowUpdateDialog] = useState(false);
  const [updatedTodo, setUpdatedTodo] = useState({
    title: todo.title,
    details: todo.details,
  });

  // EVENT HANDLERS
  function handleCheckCompleted() {
    const updatedTodos = todos.map((t) => {
      if (t.id === todo.id) {
        t.isCompleted = !t.isCompleted;
      }
      return t;
    });

    setTodos(updatedTodos);
    localStorage.setItem("todos", JSON.stringify(updatedTodos));
  }

  function handleDeleteClick() {
    setShowDeleteDialog(true);
  }

  function handleDeleteDialogClose() {
    setShowDeleteDialog(false);
  }

  function handleDeleteConfirm() {
    const updatedTodos = todos.filter((t) => t.id !== todo.id);
    setTodos(updatedTodos);
    setShowDeleteDialog(false);
    localStorage.setItem("todos", JSON.stringify(updatedTodos));
  }

  function handleUpdateDialogClose() {
    setShowUpdateDialog(false);
  }

  function handleUpdateClick() {
    setShowUpdateDialog(true);
  }

  function handleUpdateConfirm() {
    const updatedTodos = todos.map((t) => {
      if (t.id === todo.id) {
        return { ...t, title: updatedTodo.title, details: updatedTodo.details };
      }
      return t;
    });
    setTodos(updatedTodos);
    setShowUpdateDialog(false);
    localStorage.setItem("todos", JSON.stringify(updatedTodos));
  }

  // ===========EVENT HANDLERS=============

  return (
    <div>
      {/* Delete Confirmation Dialog */}
      <Dialog
        onClose={handleDeleteDialogClose}
        open={showDeleteDialog}
        aria-labelledby="alert-dialog-title"
        aria-describedby="alert-dialog-description"
      >
        <DialogTitle id="alert-dialog-title">
          Are You Sure That You Want To Delete This Mission?
        </DialogTitle>
        <DialogContent>
          <DialogContentText id="alert-dialog-description">
            This Action Cannot Be Undone. Are You Sure You Want To Delete This
            Mission?
          </DialogContentText>
        </DialogContent>
        <DialogActions>
          <Button onClick={handleDeleteDialogClose}>Cancel</Button>
          <Button autoFocus onClick={handleDeleteConfirm}>
            Delete
          </Button>
        </DialogActions>
      </Dialog>
      {/* ===Delete Confirmation Dialog=== */}

      {/* Update Dialog */}
      <Dialog
        onClose={handleUpdateDialogClose}
        open={showUpdateDialog}
        aria-labelledby="alert-dialog-title"
        aria-describedby="alert-dialog-description"
      >
        <DialogTitle id="alert-dialog-title">Edit Your Mission</DialogTitle>
        <DialogContent>
          <TextField
            autoFocus
            required
            margin="dense"
            id="name"
            name="title"
            label="Mission Title"
            type="text"
            fullWidth
            variant="standard"
            value={updatedTodo.title}
            onChange={(e) =>
              setUpdatedTodo({ ...updatedTodo, title: e.target.value })
            }
          />
          <TextField
            autoFocus
            required
            margin="dense"
            id="name"
            name="details"
            label="Mission Details"
            type="text"
            fullWidth
            variant="standard"
            value={updatedTodo.details}
            onChange={(e) =>
              setUpdatedTodo({ ...updatedTodo, details: e.target.value })
            }
          />
        </DialogContent>
        <DialogActions>
          <Button onClick={handleUpdateDialogClose}>Cancel</Button>
          <Button autoFocus onClick={handleUpdateConfirm}>
            Edit
          </Button>
        </DialogActions>
      </Dialog>
      {/* ===Update Dialog=== */}

      <Card
        className="todoCard"
        sx={{
          minWidth: 275,
          background: "#283593",
          color: "#fff",
          marginTop: "10px",
        }}
      >
        <CardContent>
          <Grid container spacing={2}>
            <Grid item size={{ xs: 6, md: 8 }}>
              <Typography variant="h5" style={{ textAlign: "left" }}>
                {todo.title}
              </Typography>
              <Typography variant="h6" style={{ textAlign: "left" }}>
                {todo.details}
              </Typography>
            </Grid>
            <Grid
              item
              size={{ xs: 6, md: 4 }}
              display="flex"
              justifyContent="space-around"
              alignItems="center"
            >
              {/* Action Buttons */}

              {/* Check Completed Button */}
              <IconButton
                onClick={() => {
                  handleCheckCompleted();
                }}
                className="iconButton"
                aria-label="check"
                style={{
                  color: "#8bc34a",
                  background: todo.isCompleted ? "#8bc34a" : "#fff",
                  border: "1px solid #8bc34a",
                }}
              >
                <CheckIcon
                  style={{ color: todo.isCompleted ? "#fff" : "#8bc34a" }}
                />
              </IconButton>
              {/* ===Check Completed Button=== */}

              {/* Edit Button */}
              <IconButton
                className="iconButton"
                aria-label="edit"
                style={{
                  color: "#1769aa",
                  background: "#fff",
                  border: "1px solid #1769aa",
                }}
                onClick={handleUpdateClick}
              >
                <ModeEditOutlineOutlinedIcon />
              </IconButton>
              {/* ===Edit Button=== */}

              {/* Delete Button */}
              <IconButton
                className="iconButton"
                aria-label="delete"
                style={{
                  color: "#b23c17",
                  background: "#fff",
                  border: "1px solid #b23c17",
                }}
                onClick={handleDeleteClick}
              >
                <DeleteOutlineOutlinedIcon />
              </IconButton>
              {/* ===Delete Button=== */}

              {/* ===Action Buttons=== */}
            </Grid>
          </Grid>
        </CardContent>
      </Card>
    </div>
  );
}