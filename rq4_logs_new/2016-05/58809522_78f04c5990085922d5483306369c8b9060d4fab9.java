public addfrd implements Command { 
addfrd x; 
public addfrd(addfrd x ){ 
addfrd=x; } 
public void execute() { 
x.add(); } 
public void unexecute() {
 x.un(); } 