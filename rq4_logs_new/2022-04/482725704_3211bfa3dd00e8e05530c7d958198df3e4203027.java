
public class AgeCalculator {
	OrderVariables variables = new OrderVariables();
	

	int ageCal () {
		if ((variables.jumin[6] - '0') == 1 || (variables.jumin[6] - '0') == 2) {
			variables.birthYear = 1900 + ((variables.jumin[0] - '0') * 10 + (variables.jumin[1] - '0'));
		} else if ((variables.jumin[6] - '0') == 3 || (variables.jumin[6] - '0') == 4) {
			variables.birthYear = 2000 + ((variables.jumin[0] - '0') * 10 + (variables.jumin[1] - '0'));
		}
		
		variables.birthMonth = ((variables.jumin[2] - '0') * 10) + (variables.jumin[3] - '0');
		variables.birthDate = ((variables.jumin[4] - '0') * 10) + (variables.jumin[5] - '0');
			
			 
			if (StaticValues.MONTH_NOW > variables.birthMonth || (StaticValues.MONTH_NOW == variables.birthMonth && StaticValues.TODAY >=variables.birthDate)) {
				variables.age = StaticValues.YEAR_NOW - variables.birthYear;
			} else {
				variables.age = StaticValues.YEAR_NOW - variables.birthYear - 1;
			}
			
			if (variables.age >=0 && variables.age < 3) {
				variables.ageGroup = StaticValues.AGE_GROUP_BABY;
			} else if (variables.age >= 3 && variables.age < 13) {
				variables.ageGroup = StaticValues.AGE_GROUP_KID;
			} else if (variables.age >= 13 && variables.age < 19 ) {
				variables.ageGroup = StaticValues.AGE_GROUP_YOUNG;
			}else if (variables.age >= 19 && variables.age < 65) {
				variables.ageGroup = StaticValues.AGE_GROUP_ADULT;
			}else if (variables.age >= 65) {
				variables.ageGroup = StaticValues.AGE_GROUP_ELDER;
		}
			return 0;
	}
}