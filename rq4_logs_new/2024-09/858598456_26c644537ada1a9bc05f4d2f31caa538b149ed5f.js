import './Bookings.css';

export default function Bookings(){
    return (
        <div className="bookings">
            <table id="contact" border="0" width="100%" cellpadding="0" cellspacing="0" bgcolor="#292929">
        <tr>
            <td>
                <table border="0" width="85%" cellpadding="15" cellspacing="0" align="center">
                    <tr>
                        <td height="180" align="center" valign="middle" colspan="1">
                                <font face="arial" color="#f3971b" size="6">
                                     BOOKING
                                </font>
                                <hr width="70" color="#f3971b" />
                        
                        </td>
                    </tr>
                    <tr>
                        <td align="center" valign="top">
                           <table border="0" width="50%" cellpadding="15" cellspacing="0" align="center" bgcolor="#353535">
                               <tr>
                                   <td width="30%">
                                       <font face="arial" size="4" color="#ffffff">
                                           Name
                                       </font>
                                   </td>
                                   <td width="70%">
                                    <font face="arial" size="4" color="#ffffff">
                                        <input type="text" size="40" />
                                    </font>
                                    </td>
                               </tr>
                               <tr>
                                <td width="30%">
                                    <font face="arial" size="4" color="#ffffff">
                                        Email
                                    </font>
                                </td>
                                <td width="70%">
                                 <font face="arial" size="4" color="#ffffff">
                                     <input type="text" size="40" />
                                 </font>
                                 </td>
                            </tr>
                            <tr>
                                <td width="30%">
                                    <font face="arial" size="4" color="#ffffff">
                                        Message
                                    </font>
                                </td>
                                <td width="70%">
                                 <font face="arial" size="4" color="#ffffff">
                                     <textarea rows="5" cols="42"></textarea>
                                 </font>
                                 </td>
                            </tr>
                            <tr>
                                <td width="30%">
                                   &nbsp;
                                </td>
                                <td width="70%">
                                    <button type="Submit">
                                        <font face="arial" size="4" color="#292929">
                                             Submit
                                        </font>
                                </button>
                                 </td>
                            </tr>
                           </table>
                        </td>
                        
                    </tr>
                    <tr>
                        <td colspan="3">
                            &nbsp;
                        </td>
                    </tr>
                </table>
            </td>

        </tr>


    </table> 
        </div>
    )
}