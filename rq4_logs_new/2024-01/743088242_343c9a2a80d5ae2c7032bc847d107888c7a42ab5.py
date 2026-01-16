from app1 import user

import numpy as np

print('''**************************************** Hi , Enter your choice to Proceed********************************************
                                                    
        **************** Check whether you are having diabetes or not using different models with their accuracy******************
                                   
                                          1 : Logistic Regrression
                                          2 : SVM
                                          3 : SVM Kernel
                                          4 : Naive Bayes
                                          5 : KNN 
                                          6 : Decision Tree
                                          7 : Random Forest

        ***********************************Check your diabetes********************************************************************

                                          8 : Check your diabetes                                  
                                                    
       ******************************Get your personalized Health status***********************************************************

                                          9 : To c1heck your health status

       ******************************Get your personalized Food Recommendation as per your diabetes level*************************
       
                                          10 : Get your diet chart

                                           ''')                                                                                 
                                                

user_choice = int(input("********************8Enter your choice (1 to 10) only Numeric*********************\n"))

print(" Ener some of the generic details to get your results\n")

user_input = [0,0,0,0,0,0,0,0]

user_input[0] = int(input(" Pregnancies: Number of times pregnant \n"))
user_input[1] = int(input("Glucose: Plasma glucose concentration a 2 hours in an oral glucose tolerance test \n"))
user_input[2] = int(input(" BloodPressure: Diastolic blood pressure (mm Hg) \n"))
user_input[3] = int(input ("SkinThickness: Triceps skin fold thickness (mm) \n"))
user_input[4] = int(input("Insulin: 2-Hour serum insulin (mu U/ml) \n"))
user_input[5] = int(input("BMI: Body mass index (weight in kg/(height in m)^2) \n"))
user_input[6] = int(input("DiabetesPedigreeFunction: Diabetes pedigree function \n"))
user_input[7] = int(input("Age: Age (years) \n"))


user_input = np.array(user_input).reshape(1, -1)
accuracy,output = user(user_choice , user_input)
if output == 1 :
        print("Person is being detected with diabetes with accuracy :")
        print(accuracy)
else:
        print("Person is not detected with diabetes with accuracy :")
        print(accuracy)


