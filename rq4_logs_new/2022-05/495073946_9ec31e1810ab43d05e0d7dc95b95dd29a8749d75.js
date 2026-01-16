
import './App.css';
import { Formik, useFormik } from 'formik';
import {useState} from 'react'

function App() {
  const [leaderBoard, setLeaderBoard] = useState([])

  const validate = values => {
    const errors = {};
    if (!values.id) {
      errors.id = 'Required';
    } else if (values.id.length > 4) {
      errors.id = 'Must be 4 characters or less';
    }
  
    if (!values.name) {
      errors.name = 'Required';
    } else if (values.name.length > 20) {
      errors.name = 'Must be 20 characters or less';
    }
  
    if (!values.steps) {
      errors.email = 'Required';
    } 
  
    return errors;
  };

  const handleSubmit = (values)=>{
    let participant_array = leaderBoard.filter((item)=>item.id==values.id);
    if (!participant_array.length){
      setLeaderBoard((prevState)=>{
        let new_arr = [...prevState,values]

        new_arr.sort((a, b) => {
          return b.steps - a.steps;
      });
      return new_arr
        
      })
    }else{
      let participant = participant_array[0]
      let updated_partipant = {
        ...participant,
        steps:parseInt(participant.steps)+parseInt(values.steps)
      }
      setLeaderBoard((prevState)=>{
        let participant_index = leaderBoard.findIndex((item)=>item.id==values.id);
        prevState.splice(participant_index,1,updated_partipant)
        
        prevState.sort((a, b) => {
          return b.steps - a.steps;
      });
      return prevState
      })
    }
    
  }


  const formik = useFormik({
    initialValues: {
      id: '',
      name: '',
      steps: '',
    },
    validate,
    onSubmit:handleSubmit
    // onSubmit: values => {
    //   console.log(values)
    //   alert(JSON.stringify(values, null, 2));
    // },
  });
  return (
    <>
      <form onSubmit={formik.handleSubmit}>
        <label htmlFor="id">ID</label>
        <input
          id="id"
          name="id"
          type="text"
          onChange={formik.handleChange}
          value={formik.values.id}
        />
        {formik.errors.id ? <div>{formik.errors.id}</div> : null}

        <label htmlFor="name">Name</label>
        <input
          id="name"
          name="name"
          type="text"
          onChange={formik.handleChange}
          value={formik.values.name}
        />
        {formik.errors.name ? <div>{formik.errors.name}</div> : null}

        <label htmlFor="steps">Steps</label>
        <input
          id="steps"
          name="steps"
          type="steps"
          onChange={formik.handleChange}
          value={formik.values.steps}
        />
        {formik.errors.steps ? <div>{formik.errors.steps}</div> : null}

        <button type="submit">Submit</button>
      </form>
      <div>
          <h2>LEADERBOARD</h2>
          {leaderBoard.map((item, index)=>(<div key={index}>
            <p>{item.name}</p>
            <p>{item.steps}</p>
          </div>))}
      </div>
    </>
  );
};


export default App;