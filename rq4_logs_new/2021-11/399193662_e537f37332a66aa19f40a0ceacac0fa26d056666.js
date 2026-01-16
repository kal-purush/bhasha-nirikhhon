import React,{useState,useEffect,useRef} from 'react'
import axios from 'axios';
import { Navbar } from '../Navbar';
import {RotateLoader} from 'react-spinners'

export const Timetable = () => {

    const [loading, setLoading] = useState(false)
    
    const override = ` 
        top: 250px;
        display: block;
        margin: 0 auto;        
    `;
    
    const [user,setUser] = useState({// *
        b1monbs:'',
        b1monbe:'',
        b1mongs:'',
        b1monge:'',
        b2monbs:'',
        b2monbe:'',
        b2mongs:'',
        b2monge:'',
        b1tuebs:'',
        b1tuebe:'',
        b1tuegs:'',
        b1tuege:'',
        b2tuebs:'',
        b2tuebe:'',
        b2tuegs:'',
        b2tuege:'',
        b1wedbs:'',
        b1wedbe:'',
        b1wedgs:'',
        b1wedge:'',
        b2wedbs:'',
        b2wedbe:'',
        b2wedgs:'',
        b2wedge:'',
        b1thurbs:'',
        b1thurbe:'',
        b1thurgs:'',
        b1thurge:'',
        b2thurbs:'',
        b2thurbe:'',
        b2thurgs:'',
        b2thurge:'',
        b2fribs:'',
        b2fribe:'',
        b2frigs:'',
        b2frige:'',
        b1satbs:'',
        b1satbe:'',
        b1satgs:'',
        b1satge:'',
        b2satbs:'',
        b2satbe:'',
        b2satgs:'',
        b1sunbs:'',
        b1sunbe:'',
        b1sungs:'',
        b1sunge:'',
        b2sunbs:'',
        b2sunbe:'',
        b2sungs:'',
        b2sunge:''
     });
  
     let name,value;
      // handling the values that are changed
     const handleinputs = (e) =>{
        name = e.target.name;
        value = e.target.value;
        setUser({...user,[name]:value})
      }
    const [id, setId] = useState('')
    useEffect(() => {
        axios.get('/users/gettimetable',{
        }).then((res)=>{        
            setLoading(true)    
            setUser({
            _id:res.data[0]._id,
            b1monbs :res.data[0].b1monbs,
            b1monbe :res.data[0].b1monbe,
            b1mongs :res.data[0].b1mongs,
            b1monge :res.data[0].b1monge,
            b2monbs :res.data[0].b2monbs,
            b2monbe :res.data[0].b2monbe,
            b2mongs :res.data[0].b2mongs,
            b2monge :res.data[0].b2monge,
            b1tuebs :res.data[0].b1tuebs,
            b1tuebe :res.data[0].b1tuebe,
            b1tuegs :res.data[0].b1tuegs,
            b1tuege :res.data[0].b1tuege,
            b2tuebs :res.data[0].b2tuebs,
            b2tuebe :res.data[0].b2tuebe,
            b2tuegs :res.data[0].b2tuegs,
            b2tuege :res.data[0].b2tuege,
            b1wedbs :res.data[0].b1wedbs,
            b1wedbe:res.data[0]. b1wedbe,
            b1wedgs :res.data[0].b1wedgs,
            b1wedge :res.data[0].b1wedge,
            b2wedbs :res.data[0].b2wedbs,
            b2wedbe :res.data[0].b2wedbe,
            b2wedgs :res.data[0].b2wedgs,
            b2wedge :res.data[0].b2wedge,
            b1thurbs:res.data[0]. b1thurbs,
            b1thurbe:res.data[0]. b1thurbe,
            b1thurgs:res.data[0]. b1thurgs,
            b1thurge:res.data[0]. b1thurge,
            b2thurbs:res.data[0]. b2thurbs,
            b2thurbe:res.data[0]. b2thurbe,
            b2thurgs:res.data[0]. b2thurgs,
            b2thurge:res.data[0]. b2thurge,            
            b1fribs :res.data[0].b1fribs,
            b1fribe :res.data[0].b1fribe,
            b1frigs :res.data[0].b1frigs,
            b1frige :res.data[0].b1frige,
            b2fribs :res.data[0].b2fribs,
            b2fribe :res.data[0].b2fribe,
            b2frigs :res.data[0].b2frigs,
            b2frige :res.data[0].b2frige,
            b1satbs :res.data[0].b1satbs,
            b1satbe :res.data[0].b1satbe,
            b1satgs :res.data[0].b1satgs,
            b1satge :res.data[0].b1satge,
            b2satbs :res.data[0].b2satbs,
            b2satbe :res.data[0].b2satbe,
            b2satgs :res.data[0].b2satgs,
            b2satge :res.data[0].b2satge,
            b1sunbs :res.data[0].b1sunbs,
            b1sunbe :res.data[0].b1sunbe,
            b1sungs :res.data[0].b1sungs,
            b1sunge:res.data[0].b1sunge,
            b2sunbs :res.data[0].b2sunbs,
            b2sunbe :res.data[0].b2sunbe,
            b2sungs :res.data[0].b2sungs,
            b2sunge : res.data[0].b2sunge
        })
        setId(res.data[0]._id)
        }).catch((err)=>{

        })
    }, [])

    const makeupdate = async (e) => {
        e.preventDefault();                
        const {b1monbs,b1monbe,b1mongs,b1monge,b2monbs,b2monbe,b2mongs,b2monge,b1tuebs,b1tuebe,b1tuegs,b1tuege,b2tuebs,b2tuebe,b2tuegs,b2tuege,b1wedbs,b1wedbse,b1wedgs,b1wedge,b2wedbs,b2wedbe,b2wedgs,b2wedge,b1thurbs,b1thurbe,b1thurgs,b1thurge,b2thurbs,b2thurbe,b2thurgs,b2thurge,b1frie,b1fris,b2fribs,b2fribe,b2frigs,b2frige,b1satbs,b1satbe,b1satgs,b1satge,b2satbs,b2satbe,b2satgs,b2satge,b1sunbs,b1sunbe,b1sungs,b1sungse,b2sunbs,b2sunbe,b2sungs,b2sunge} = user;
            console.log(user)
        await axios.patch(`/users/updatetimetable/${id}`,{
            b1monbs,b1monbe,b1mongs,b1monge,b2monbs,b2monbe,b2mongs,b2monge,b1tuebs,b1tuebe,b1tuegs,b1tuege,b2tuebs,b2tuebe,b2tuegs,b2tuege,b1wedbs,b1wedbse,b1wedgs,b1wedge,b2wedbs,b2wedbe,b2wedgs,b2wedge,b1thurbs,b1thurbe,b1thurgs,b1thurge,b2thurbs,b2thurbe,b2thurgs,b2thurge,b1frie,b1fris,b2fribs,b2fribe,b2frigs,b2frige,b1satbs,b1satbe,b1satgs,b1satge,b2satbs,b2satbe,b2satgs,b1sunbs,b1sunbe,b1sungs,b1sungse,b2sunbs,b2sunbe,b2sungs,b2sunge
        }).then((res)=>{
                console.log(res);
                alert('done');
        }).catch((err) => {
                alert('err');
                console.log(err); 
        });
    }
  
    return (
        <div>
            <div className="wrapper">
                <Navbar/>
                <div id="content-page" class="content-page">
                    <div class="container-fluid">
                        <div className="row">
                            <div className="col-sm-12">
                            {loading ?
                            <div class="iq-card">
                                <div class="iq-card-header d-flex justify-content-between">
                                    <div class="iq-header-title">
                                        <h3 class="card-title">Editable Time Table :</h3>
                                    </div>                                    
                                </div>
                                <p className="ml-4">Click on any cell to update the data & then click on Update.</p>
                                <form onSubmit={makeupdate}>                                
                                <span class="table-add float-right mb-3 m-2">
                                    <button class="btn btn-lg iq-bg-success"><span class="pl-1">Update</span>
                                    </button>
                                </span>                      
                                <div class="iq-card-body">
                                <h5 className="m-2">Physiotherapy branch Time Table!</h5>                                
                                <div id="table" class="table-editable">          
                                <table className="table table-bordered table-responsive-md table-striped text-center">
                                    <thead>
                                    <tr>
                                        <th>Mon</th>
                                        <th>Tue</th>
                                        <th>Wed</th>
                                        <th>Thur</th>
                                        <th>Fri</th>
                                        <th>Sat</th>
                                        <th>Sun</th>
                                    </tr>
                                    </thead>
                                    <tbody className="text-center">
                                        <tr>
                                            <td>
                                                <b>Boys</b> (Evening):<br/>
                                                <span contenteditable="true">
                                                <input className="text-center form-control" type="text" name="b1monbs" value={user.b1monbs} onChange={handleinputs}/></span> to <span contenteditable="true"></span><br/>
                                                <input className="text-center form-control" type="text" value={user.b1monbe} name="b1monbe" onChange={handleinputs} />
                                                <hr/>
                                                <b>Girls</b> (Morning):<br/>
                                                <span contenteditable="true"><input className="text-center form-control" type="text" value={user.b1mongs} name="b1mongs" onChange={handleinputs} /></span> to <span contenteditable="true"></span><br/>
                                                <input className="text-center form-control" type="text" value={user.b1monge} name="b1monge" onChange={handleinputs} /><br/>
                                            </td>
                                            <td>
                                                <b>Boys</b> (Evening):<br/>
                                                <span contenteditable="true"><input className="text-center form-control" type="text" value={user.b1tuebs} name="b1tuebs" onChange={handleinputs} /></span> to <span contenteditable="true"></span><br/>
                                                <input className="text-center form-control" type="text" value={user.b1tuebe} name="b1tuebe" onChange={handleinputs} />
                                                <hr/>
                                                <b>Girls</b> (Morning):<br/>
                                                <span contenteditable="true"><input className="text-center form-control" type="text" value={user.b1tuegs} name="b1tuegs" onChange={handleinputs} /></span> to <span contenteditable="true"></span><br/>
                                                <input className="text-center form-control" type="text" value={user.b1tuege} name="b1tuege" onChange={handleinputs} /><br/>
                                            </td>
                                            <td>
                                                <b>Boys</b> (Evening):<br/>
                                                <span contenteditable="true"><input className="text-center form-control" type="text" value={user.b1wedbs} name="b1wedbs" onChange={handleinputs} /></span> to <span contenteditable="true"></span><br/>
                                                <input className="text-center form-control" type="text" value={user.b1wedbe} name="b1wedbe" onChange={handleinputs} />
                                                <hr/>
                                                <b>Girls</b> (Morning):<br/>
                                                <span contenteditable="true"><input className="text-center form-control" type="text" value={user.b1wedgs} name="b1wedgs" onChange={handleinputs} /></span> to <span contenteditable="true"></span><br/>
                                                <input className="text-center form-control" type="text" value={user.b1wedge} name="b1wedge" onChange={handleinputs} /><br/>
                                            </td>
                                            <td>
                                                <b>Boys</b> (Evening):<br/>
                                                <span contenteditable="true"><input className="text-center form-control" type="text" value={user.b1thurbs} name="b1thurbs" onChange={handleinputs} /></span> to <span contenteditable="true"></span><br/>
                                                <input className="text-center form-control" type="text" value={user.b1thurbe} name="b1thurbe" onChange={handleinputs} />
                                                <hr/>
                                                <b>Girls</b> (Morning):<br/>
                                                <span contenteditable="true"><input className="text-center form-control" type="text" value={user.b1thurgs} name="b1thurgs" onChange={handleinputs} /></span> to <span contenteditable="true"></span><br/>
                                                <input className="text-center form-control" type="text" value={user.b1thurge} name="b1thurge" onChange={handleinputs} /><br/>
                                            </td>
                                            <td>
                                                <b>Boys</b> (Evening):<br/>
                                                <span contenteditable="true"><input className="text-center form-control" type="text" value={user.b1fribs} name="b1fribs" onChange={handleinputs} /></span> to <span contenteditable="true"></span><br/>
                                                <input className="text-center form-control" type="text" value={user.b1fribe} name="b1fribe" onChange={handleinputs} />
                                                <hr/>
                                                <b>Girls</b> (Morning):<br/>
                                                <span contenteditable="true"><input className="text-center form-control" type="text" value={user.b1frigs} name="b1frigs" onChange={handleinputs} /></span> to <span contenteditable="true"></span><br/>
                                                <input className="text-center form-control" type="text" value={user.b1frige} name="b1frigs" onChange={handleinputs} /><br/>
                                            </td>
                                            <td>
                                                <b>Boys</b> (Evening):<br/>
                                                <span contenteditable="true"><input className="text-center form-control" type="text" value={user.b1satbs} name="b1satbs" onChange={handleinputs} /></span> to <span contenteditable="true"></span><br/>
                                                <input className="text-center form-control" type="text" value={user.b1satbe} name="b1satbe" onChange={handleinputs} />
                                                <hr/>
                                                <b>Girls</b> (Morning):<br/>
                                                <span contenteditable="true"><input className="text-center form-control" type="text" value={user.b1satgs} name="b1satgs" onChange={handleinputs} /></span> to <span contenteditable="true"></span><br/>
                                                <input className="text-center form-control" type="text" value={user.b1satge} name="b1satge" onChange={handleinputs} /><br/>
                                            </td>
                                            <td>
                                                <b>Boys</b> (Evening):<br/>
                                                <span contenteditable="true"><input className="text-center form-control" type="text" value={user.b1sunbs} name="b1sunbs" onChange={handleinputs} /></span> to <span contenteditable="true"></span><br/>
                                                <input className="text-center form-control" type="text" value={user.b1sunbe} name="b1sunbe" onChange={handleinputs} />
                                                <hr/>
                                                <b>Girls</b> (Morning):<br/>
                                                <span contenteditable="true"><input className="text-center form-control" type="text" value={user.b1sungs} name="b1sungs" onChange={handleinputs} /></span> to <span contenteditable="true"></span><br/>
                                                <input className="text-center form-control" type="text" value={user.b1sunge} name="b1sunge" onChange={handleinputs} /><br/>
                                            </td>                                            
                                        </tr>                                    
                                    </tbody>
                                </table>
                                </div>
                                </div>

                                <div class="iq-card-body">                                
                                    <h5 className="m-2">Canteen branch Time Table!</h5>
                                    <div id="table" class="table-editable">                                    
                                    <table class="table table-bordered table-responsive-md table-striped text-center">
                                        <thead>
                                        <tr>
                                            <th>Mon</th>
                                            <th>Tue</th>
                                            <th>Wed</th>
                                            <th>Thur</th>
                                            <th>Fri</th>
                                            <th>Sat</th>
                                            <th>Sun</th>                                            
                                        </tr>
                                        </thead>
                                        <tbody>
                                            <tr>
                                                <td>
                                                    <b>Boys</b> (Morning):<br/>
                                                    <span contenteditable="true"><input className="text-center form-control" type="text" value={user.b2monbs} name="b2monbs" onChange={handleinputs} /></span> to <span contenteditable="true"></span><br/>
                                                    <input className="text-center form-control" type="text" value={user.b2monbe} name="b2monbe" onChange={handleinputs} />
                                                    <hr/>
                                                    <b>Girls</b> (Evening):<br/>
                                                    <span contenteditable="true"><input className="text-center form-control" type="text" value={user.b2mongs} name="b2mongs" onChange={handleinputs} /></span> to <span contenteditable="true"></span><br/>
                                                    <input className="text-center form-control" type="text" value={user.b2monge} name="b2monge" onChange={handleinputs} /><br/>
                                                </td>
                                                <td>
                                                    <b>Boys</b> (Morning):<br/>
                                                    <span contenteditable="true"><input className="text-center form-control" type="text" value={user.b2tuebs} name="b2tuebs" onChange={handleinputs} /></span> to <span contenteditable="true"></span><br/>
                                                    <input className="text-center form-control" type="text" value={user.b2tuebe} name="b2tuebe" onChange={handleinputs} />
                                                    <hr/>
                                                    <b>Girls</b> (Evening):<br/>
                                                    <span contenteditable="true"><input className="text-center form-control" type="text" value={user.b2tuegs} name="b2tuegs" onChange={handleinputs} /></span> to <span contenteditable="true"></span><br/>
                                                    <input className="text-center form-control" type="text" value={user.b2tuege} name="b2tuege" onChange={handleinputs} /><br/>
                                                </td>
                                                <td>
                                                    <b>Boys</b> (Morning):<br/>
                                                    <span contenteditable="true"><input className="text-center form-control" type="text" value={user.b2wedbs} name="b2wedbs" onChange={handleinputs} /></span> to <span contenteditable="true"></span><br/>
                                                    <input className="text-center form-control" type="text" value={user.b2wedbe} name="b2wedbe" onChange={handleinputs} />
                                                    <hr/>
                                                    <b>Girls</b> (Evening):<br/>
                                                    <span contenteditable="true"><input className="text-center form-control" type="text" value={user.b2wedgs} name="b2wedgs" onChange={handleinputs} /></span> to <span contenteditable="true"></span><br/>
                                                    <input className="text-center form-control" type="text" value={user.b2wedge} name="b2wedge" onChange={handleinputs} /><br/>
                                                </td>
                                                <td>
                                                    <b>Boys</b> (Morning):<br/>
                                                    <span contenteditable="true"><input className="text-center form-control" type="text" value={user.b2thurbs} name="b2thurbs" onChange={handleinputs} /></span> to <span contenteditable="true"></span><br/>
                                                    <input className="text-center form-control" type="text" value={user.b2thurbe} name="b2thurbe" onChange={handleinputs} />
                                                    <hr/>
                                                    <b>Girls</b> (Evening):<br/>
                                                    <span contenteditable="true"><input className="text-center form-control" type="text" value={user.b2thurgs} name="b2thurgs" onChange={handleinputs} /></span> to <span contenteditable="true"></span><br/>
                                                    <input className="text-center form-control" type="text" value={user.b2thurge} name="b2thurge" onChange={handleinputs} /><br/>
                                                </td>
                                                <td>
                                                    <b>Boys</b> (Morning):<br/>
                                                    <span contenteditable="true"><input className="text-center form-control" type="text" value={user.b2fribs}name="b2fribs" onChange={handleinputs} /></span> to <span contenteditable="true"></span><br/>
                                                    <input className="text-center form-control" type="text" value={user.b2fribe}name="b2fribe" onChange={handleinputs} />
                                                    <hr/>
                                                    <b>Girls</b> (Evening):<br/>
                                                    <span contenteditable="true"><input className="text-center form-control" type="text" value={user.b2frigs}name="b2frigs" onChange={handleinputs} /></span> to <span contenteditable="true"></span><br/>
                                                    <input className="text-center form-control" type="text" value={user.b2frige}ame="b2frige" onChange={handleinputs} /><br/>
                                                </td>
                                                <td>
                                                    <b>Boys</b> (Morning):<br/>
                                                    <span contenteditable="true"><input className="text-center form-control" type="text" value={user.b2satbs} name="b2satbs" onChange={handleinputs} /></span> to <span contenteditable="true"></span><br/>
                                                    <input className="text-center form-control" type="text" value={user.b2satbe} name="b2satbe" onChange={handleinputs} />
                                                    <hr/>
                                                    <b>Girls</b> (Evening):<br/>
                                                    <span contenteditable="true"><input className="text-center form-control" type="text" value={user.b2satgs} name="b2satgs" onChange={handleinputs} /></span> to <span contenteditable="true"></span><br/>
                                                    <input className="text-center form-control" type="text" value={user.b2satge} name="b2satge" onChange={handleinputs} /><br/>
                                                </td>
                                                <td>
                                                    <b>Boys</b> (Morning):<br/>
                                                    <span contenteditable="true"><input className="text-center form-control" type="text" value={user.b2sunbs} name="b2sunbs" onChange={handleinputs} /></span> to <span contenteditable="true"></span><br/>
                                                    <input className="text-center form-control" type="text" value={user.b2sunbe} name="b2sunbe" onChange={handleinputs} />
                                                    <hr/>
                                                    <b>Girls</b> (Evening):<br/>
                                                    <span contenteditable="true"><input className="text-center form-control" type="text" value={user.b2sungs} name="b2sunbs" onChange={handleinputs} /></span> to <span contenteditable="true"></span><br/>
                                                    <input className="text-center form-control" type="text" value={user.b2sunge} name="b2sunbe" onChange={handleinputs} /><br/>
                                                </td>                                                
                                            </tr>                                    
                                        </tbody>
                                    </table>
                                    </div>
                                </div>
                                    </form>
                        </div>
                        :<RotateLoader size={20} color='#9d7af3' css={override}  loading/>}
                            </div>
                        </div>
                    </div>
                </div>
            </div>
        </div>
    )
}