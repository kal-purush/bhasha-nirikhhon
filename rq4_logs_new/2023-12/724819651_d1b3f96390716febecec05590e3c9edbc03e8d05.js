import { useState } from "react";

const Form = () =>{
    const [formData, setFormData]= useState({
        player1:"",
        player2:""
    })
    const handleInputChange = (e) => {
        const { name, value } = e.target;
        setFormData({ ...formData, [name]: value });
      };
    
     
      const handleSubmit = (e) => {
        e.preventDefault();
        console.log("Form submitted:", formData);
      };
    return(
        <form onSubmit={handleSubmit}>
            <div>
            <label>Player1:
            <input 
            type="text"
            name="player1"
            value={formData.player1}
            onChange={handleInputChange} />
            </label>
            </div>
            <br />
            <div>
                <label>Player2: 
                    <input
                    type="text"
                    name="player2"
                    value={formData.player2}
                    onChange={handleInputChange} />
                </label>
            </div>
            <br />
            <button type="submit"> submit</button>

            </form>
    )
}







export default Form