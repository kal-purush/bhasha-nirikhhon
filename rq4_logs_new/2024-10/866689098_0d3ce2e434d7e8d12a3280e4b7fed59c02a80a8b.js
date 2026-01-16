import React, { useState, useRef } from 'react';
import axios from 'axios';

function AddTaskModal({ getTaskData }) {
  const [formValue, setFormValue] = useState({ taskName: '', taskInfo: '' });
  const modalRef = useRef();

  const handelInput = (inputEvent) => {
    setFormValue({ ...formValue, [inputEvent.target.name]: inputEvent.target.value });
  };

  const handleSubmit = async (submitEvent) => {
    console.log(submitEvent)
    console.log(submitEvent.preventDefault())
    submitEvent.preventDefault();
    const formData = { taskName: formValue.taskName, taskInfo: formValue.taskInfo };
    const res = await axios.post("http://localhost/reactcrudphp/api/tasks.php", formData);

    if (res.data.succes) {
      const bootstrapModal = window.bootstrap.Modal.getInstance(modalRef.current);
      bootstrapModal.hide();
      
      getTaskData();
    }
  };

  return (
    <div className="modal" id="addTaskModal" ref={modalRef}>
      <div className="modal-dialog">
        <div className="modal-content">

          <div className="modal-header">
            <h4 className="modal-title">Add a task</h4>
            <button type="button" className="btn-close" data-bs-dismiss="modal"></button>
          </div>

          <div className="modal-body">
            <form onSubmit={handleSubmit}>
              <label htmlFor="taskName">Task Name</label>
              <input 
                className="form-control mb-2" 
                type="text" 
                value={formValue.taskName} 
                name="taskName" 
                onChange={handelInput} 
              />
              <label htmlFor="taskInfo">Task Info</label>
              <textarea 
                className="form-control mb-2" 
                value={formValue.taskInfo} 
                name="taskInfo" 
                onChange={handelInput} 
              />
              <input className="mt-2 form-control btn btn-success" type="submit" />
            </form>
          </div>

          <div className="modal-footer">
            <button type="button" className="btn btn-danger" data-bs-dismiss="modal">Close</button>
          </div>

        </div>
      </div>
    </div>
  );
}

export default AddTaskModal;