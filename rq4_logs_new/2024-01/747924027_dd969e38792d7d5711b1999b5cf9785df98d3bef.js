(function () {
	let arrStudents = [];
	let num = arrStudents.length + 1;

function formHandler() {
    
    const name = document.getElementById("inputDateName").value.trim();
    const surname = document.getElementById('inputDateSurname').value.trim();
    const patronymic = document.getElementById('inputDatePatronymic').value.trim();
		const dateBirth = new Date(document.getElementById('inputDateBirth').value.trim());
    const studYear = parseInt(document.getElementById('inputDateYear').value.trim());
    const faculty = document.getElementById('inputDateFaculty').value.trim();

		const errorMessageElement = document.getElementById('errorMessage');
		errorMessageElement.innerHTML = '';

		if (!name || !surname || !patronymic || !patronymic || !dateBirth || !studYear || !faculty) {
			errorMessageElement.innerHTML = 'Все поля обязательны для заполнения.';
			return;
		}

		const dDate = new Date(dateBirth);
		const currentYear = new Date().getFullYear();

		if (dDate < new Date('1900-01-01') || dDate > new Date()) {
			errorMessageElement.innerHTML = 'Дата рождения должна быть в диапазоне от 01.01.1900 до текущей даты.';
			return;
		}

		if (studYear < 1930 || studYear > currentYear) {
			errorMessageElement.innerHTML = 'Год начала обучения должен быть в диапазоне от 2000 до текущего года.';
			return;
		}

    const student = {
        filterName: name,
        surname: surname,
        patronymic: patronymic,
        dateBirth: dateBirth,
        studYear: studYear,
        faculty: faculty
    };
		console.log(student);
    return student;
}


	function getStudentItem(student) {
			const age = new Date().getFullYear() - new Date(student.dateBirth).getFullYear();
			const birth = new Date(student.dateBirth);
      const endYear = student.studYear + 4;
      const course = endYear <= new Date().getFullYear() ? 'закончил' : `${new Date().getFullYear() - student.studYear + 1} курс`;
      return `
        <tr>
					<th scope='row'>${num++}</th>
          <td>${student.filterName}</td>
          <td>${student.surname}</td>
          <td>${student.patronymic}</td>
          <td>${birth.toDateString()} (${age} лет)</td>
          <td>${student.studYear}-${endYear} (${course})</td>
					<td>${student.faculty}</td>
        </tr>
      `;
	}

	document.addEventListener('DOMContentLoaded', function () {
		const form = document.getElementById('studentForm');
		form.addEventListener('submit', function (event) {
			event.preventDefault();
	
			const student = formHandler();
			if (student) {
				arrStudents.push(student);
	
				const tableBody = document.getElementById('tBody');
				const newItem = getStudentItem(student);
				tableBody.innerHTML += newItem;
	
				form.reset();
			}  else {
				console.error('Table body element not found');
		}
		});
	});
})();