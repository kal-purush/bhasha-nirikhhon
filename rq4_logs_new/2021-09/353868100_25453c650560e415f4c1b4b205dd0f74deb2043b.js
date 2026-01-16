import React from 'react';

const Modal = () => {
  return (
    <div
      className='modal fade'
      id='exampleModal'
      tabIndex='-1'
      aria-labelledby='exampleModalLabel'
      aria-hidden='true'
    >
      <div className='modal-dialog'>
        <div className='modal-content'>
          <div className='modal-header'>
            <span className='modal-title' id='exampleModalLabel'>
              Send me a message!
            </span>
            <button
              type='button'
              className='btn-close'
              data-bs-dismiss='modal'
              aria-label='Close'
            ></button>
          </div>
          <div className='modal-body'>
            <div className='d-flex align-items-center'>
              <a
                role='button'
                className='mx-2 fs-1'
                href='https://www.linkedin.com/in/daniel-ramirez-salazar-800081145'
                target='_blank'
                rel='noreferrer'
                data-bs-toggle='tooltip'
                data-bs-placement='Bottom'
                title='Check my linkedIn account!'
              >
                <i className='fab fa-linkedin  text-decoration-none ' />
              </a>
              <span>Lets talk on linkedIn</span>
            </div>
            <div className='d-flex align-items-center'>
              <a
                role='button'
                className='mx-2 fs-1'
                href='mailto: danielrs9504@gmail.com'
                target='_blank'
                rel='noreferrer'
                data-bs-toggle='tooltip'
                data-bs-placement='Bottom'
                title='Check my linkedIn account!'
              >
                <i className='fas fa-envelope-square text-decoration-none'></i>
              </a>
              <span>Send email at danielrs9504@gmail.com</span>
            </div>
          </div>
          {/* <div className='modal-footer'>
            <button
              type='button'
              className='btn btn-secondary'
              data-bs-dismiss='modal'
            >
              Close
            </button>
            <button type='button' className='btn btn-primary'>
              Save changes
            </button>
          </div> */}
        </div>
      </div>
    </div>
  );
};

export default Modal;