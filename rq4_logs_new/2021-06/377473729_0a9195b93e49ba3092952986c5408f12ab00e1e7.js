const express = require('express')
const bodyParser=require('body-parser');
const ejs=require('ejs');
const app = express()


app.use(express.static("public"));
app.set('view engine', 'ejs');

app.use(bodyParser.urlencoded({extended: true}));
app.use(express.static("public"));

app.get('/', function(req, res){
    res.sendFile(__dirname+"/index.html")
});

app.get('/form', function(req, res){
    res.sendFile(__dirname+"/form.html")
});

app.post('/resume',function(req,res){
  const skills=[];
  const experiences=[];
  const extraActivity=[];
  const projects=[];
  const projectDetails=[];
  const name=req.body.name;
  const email=req.body.email;
  const phonenmbr=req.body.number;
  const summary=req.body.sum;
  const branch=req.body.branch;
  const college={
    name:req.body.college[0],
    marks:req.body.college[1],
    year:req.body.college[2],
  };

  const hschool={
    name:req.body.Hschool[0],
    marks:req.body.Hschool[1],
    year:req.body.Hschool[2],
  };

  const school={
    name:req.body.school[0],
    marks:req.body.school[1],
    year:req.body.school[2],
  };


 for(var i=0;i<req.body.skill.length;i++)
 {
   if(req.body.skill[i]!="")
    skills.push(req.body.skill[i]);
 }

 for(var i=0;i<req.body.exp.length;i++)
 {
   if(req.body.exp[i]!="")
    experiences.push(req.body.exp[i]);
 }

 for(var i=0;i<req.body.act.length;i++)
 {
   if(req.body.act[i]!="")
    extraActivity.push(req.body.act[i]);
 }


 for(var i=0;i<req.body.proj.length;i++)
 {
   if(req.body.proj[i]!="")
    projects.push(req.body.proj[i]);
 }

 for(var i=0;i<req.body.projdet.length;i++)
 {
   if(req.body.projdet[i]!="")
    projectDetails.push(req.body.projdet[i]);
 }


  res.render("resume",{name:name,num:phonenmbr,branch:branch,email:email,college:college.name,yearCollege:college.year,
    cgpaCollege:college.marks,yearHschool:hschool.year,Hschool:hschool.name,Hschool_mark:hschool.marks,yearSchool:school.year,
    school:school.name,school_marks:school.marks,objective:summary,projects:projects,
    projectsdet:projectDetails,skills:skills,extraActivity:extraActivity,
  });
  //res.sendFile(__dirname+"/resume.html");
});

app.listen(process.env.PORT||3000,function() {
  console.log(`Example app listening`);
});