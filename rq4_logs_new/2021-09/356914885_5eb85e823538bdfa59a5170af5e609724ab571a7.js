const { query } = require('express');
const e = require('express');
const fs = require('fs');





module.exports = {
    reserveWorkspacePage: (req, res) =>{
        let workspaceId = req.params.workspace_no;
        let query = "SELECT * FROM workspace WHERE workspace_no = '"+ workspaceId +"' ";

        
        

        db.query(query, (err, result) => {
            if (err) {
                return res.status(500).send(err);
            }

            res.render('reserve-workspace.ejs', {
                title: "Welcome to Spaceto | edit of Workspaces",
                reserve: result[0],
                message: ''
                
                
            });
        });
    
    },
    reserveWorkspace: (req, res) =>{
        let workspaceId = req.params.workspace_no;
        let reservedate = req.body.reservedate;
        let timestart = req.body.timestart;
        let timeend = req.body.timeend;
        let reserve_datetime_start =reservedate+" "+timestart;
        let reserve_datetime_end =reservedate+" "+timeend;
        
        
        
        
        let reservecheck ="select * from reserve where workspace_no = "+ workspaceId + " and reserve_datetime_start <= '"+ reserve_datetime_start + "' and reserve_datetime_end >= '"+ reserve_datetime_end +"'"
        db.query(reservecheck, (err,result) => {
            if(err){
                return res.status(500).send(err);
            }

            if (result.length > 0) {
                message = "this time already reserved";
                res.render('reserve-workspace.ejs', {
                    message,
                    title: "Welcome to Spaceto | Add a new Workspaces"
                });

                 

            }else{
                let insertreserve = "insert reserve set workspace_no = '" + workspaceId + "', reserve_datetime_start = '" + reserve_datetime_start + "', reserve_datetime_end = '" + reserve_datetime_end + "' "
                 db.query(insertreserve, (err,result) => {
                 if(err){
                    return res.status(500).send(err);
                }
                console.log(reservecheck+"+++++++");
                    res.redirect('/');

            
            });


                }



        });
        

}


}