import '../modal.css'
import { Modal ,Button} from 'react-bootstrap';
import styled ,{css} from'styled-components'
const Modalcomponent=(props)=>{
  const Custommodal=styled(Modal)`
  width: 400px;
  `
return(<>

{/* <h1>hello</h1>
<div className="modal_custom">
        <div className="modal-conten">
          <div className="modal-close-button-containe">
            <button onClick={props.onClose}>X</button>
          </div>
        "hi"
          {props.children}
        </div>
      </div> */}
         <Modal
      {...props}
      size="lg"
      aria-labelledby="contained-modal-title-vcenter"
      centered
    >
      <Modal.Header closeButton>
        <Modal.Title id="contained-modal-title-vcenter">
        </Modal.Title>
      </Modal.Header>
      <Modal.Body>
        {props.children}
      </Modal.Body>
      <Modal.Footer>
        <Button onClick={props.onHide}>Close</Button>
      </Modal.Footer>
    </Modal>
</>
)
}
export default Modalcomponent