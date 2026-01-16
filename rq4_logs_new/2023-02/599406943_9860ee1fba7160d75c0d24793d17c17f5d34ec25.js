import React from 'react'

export default function Alert(props) {
  return  (
    <div style={{height:'15px'}}>
      {
        props.mesg && <div style={{backgroundColor: 'cyan'}}>
          {props.mesg.msg} {/* yaha props.msg nhi likha bca props me mesg naam ka object aa raha hai. mesg obj ka element hai msg */}
        </div>
      }
    </div>
  )
}