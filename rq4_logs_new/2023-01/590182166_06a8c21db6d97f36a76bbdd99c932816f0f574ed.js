let grade;
    let updatedGrades = [];
    for (let i = 0; i < grades.length; i++) {
        grade = grades[i];
        for (let j = 0; j < 2; j++) {
            if (grade >= 38 && grade % 5 >= 3) {
                grade++
            }
        }
        updatedGrades.push(grade)
    }