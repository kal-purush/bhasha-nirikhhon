import React, {useState} from 'react'
import axios from 'axios';
import EditDoctors from './EditDoctors';
import EditPatient from './EditPatient';
import EditReceptionist from './EditReceptionist';

const TableItems = (props) => {

  const [modal, setModal] = useState();

  const editRec = () => {
    setModal(<EditReceptionist upRen={props.rerender} rerender={setModal} updateName={props.name} updateSurname={props.surname} updateSpecialization={props.specialization} updateDepartment={props.department} updateEmail={props.email} updateContact={props.contact} id={props.uniqueId} />)
  }

  const deletBooking = () => {
    if(window.confirm("Is the appointmnet complete?") == true){
      
      let postId = {id: props.uniqueId}

    axios.post('http://localhost:80/drOffice/deletBooking.php', postId)
    .then((response) => {
      let data = response.data;
      console.log(data);
      props.rerender(true);
      console.log("The usere deleted")
    });
      
    } else {
      console.log("The usere did not delete the post")
    }
  }

  const deletPat = () => {
    if(window.confirm("Are you sure you want to delete the post") == true){
      
      let postId = {id: props.uniqueId}

    axios.post('http://localhost:80/drOffice/deletPat.php', postId)
    .then((response) => {
      let data = response.data;
      console.log(data);
      props.rerender(true);
      console.log("The usere deleted")
    });
      
    } else {
      console.log("The usere did not delete the post")
    }
  }

  const deletRec = () => {
    if(window.confirm("Are you sure you want to delete the post") == true){
      
      let postId = {id: props.uniqueId}

    axios.post('http://localhost:80/drOffice/deletRec.php', postId)
    .then((response) => {
      let data = response.data;
      console.log(data);
      props.rerender(true);
      console.log("The usere deleted")
    });
      
    } else {
      console.log("The usere did not delete the post")
    }
  }

  const drImg = 'http://localhost:80/drOffice/' + props.profileImg;
  // let source = data[0].imgPath;
  //     let renderpath = 'http://localhost:80/apiWeek8/' + source;

  return (
    <>
      {modal}
          <tr>
            <td>{props.doctor}</td>
            <td>{props.patient}</td>
            <td>{props.date}</td>
            <td>{props.time}</td>
            <td>{props.receptionist}</td>
            <td>{props.room}</td>

            <td className='tb-change'><img className="change-img" src='../done.png' onClick={deletBooking}/></td>

          </tr>       
        {/* <p className='edit' onClick={editPost}>Edit Post</p>
        <p className='delete' onClick={deletPost}>Delete Post</p> */}
    </>
  )
}

export default TableItems 