class Plus : IOperation
    {
        private readonly int my_opp_id;
        private float current_status;

        public Plus()
        {
            my_opp_id = 1; //version number
 current_status=0; //begin
        }
--------------------------------------------------------------------
        public double result {
            get
            {
                if (variable_1 != null && variable_2 != null)
                    return variable_1 + variable_2;
                throw new Exception("variable is empty");

    data_validation();
	    retrun add(variable_1 ,variable_2);

            }
            set { throw new Exception("FIG VAM!!!"); 
result_validation_set()}
        }
        public double variable_1 { get; set; }
        public double variable_2 { get; set; }
        public int opp_id {
            get { return my_opp_id; }
            set { throw new Exception(); }
        }
public float status{ get{ return current_status;}; set; }

----------------------------------------------------------------------

	private void result_validation_set(){
throw new Exception("FIG VAM!!!");
}
private void data_validation(){
	if (Double.IsNaN(variable_1){
		console.writeline(“Warning!. Variable 1 was emty. By default it was taken zero”);
		variable_1=0;

}

if if (Double.IsNaN(variable_2){
		console.writeline(“Warning!. Variable 2 was emty. By default it was taken zero”);
		variable_2=0;

}
//Exeption part…...Error


        current_status=0.5; //half


}
	private double add(double a, double b){
			current_status=1; //end

			return a+b;
})
    }