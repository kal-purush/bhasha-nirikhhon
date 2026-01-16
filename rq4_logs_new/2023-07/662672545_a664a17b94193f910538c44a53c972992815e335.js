import { useState } from "react";

function FormElmnt(){
    const genders=['Kadın','Erkek'];

    const [name, setName]=useState('');
    const [description, setDescription]=useState('');
    const [gender, setGender]=useState('0');
    const [rule, setRule]=useState(false);
    return(
        <>
            <button onClick={()=> setName('Ahmet')}>Adı değiştir</button>

            <input type="text" value={name} onChange={e=> setName(e.target.value)}/>
            <br/>
            {name}
            <br></br>
            <textarea value={description} onChange={e=> setDescription(e.target.value)}></textarea>
            <br></br>
            {description}
            <select value={gender} onChange={e=> setGender(e.target.value)}>
                <option value={'Cinsiyet Seçin'}>Cinsiyet Seçin</option>
                {genders.map((gender,index)=>(
                    <option value={gender} key={index}>{gender}</option>
                ))}
            </select>
            {gender}
            <br/>
            <label>
                <input type="checkbox" checked={rule} onChange={e=> setRule(e.target.checked)} />Sözleşmeyi kabul edin
            </label>
            <br/>
            <button disabled={!rule}>Devam et</button>
            {rule && <p>anan</p>}
        </>
    )
}

export default FormElmnt