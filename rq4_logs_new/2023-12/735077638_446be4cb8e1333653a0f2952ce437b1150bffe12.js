const form = document.querySelector('form');
    const emailError = document.querySelector('.email-error');
    const passwordError = document.querySelector('.password-error');
    form.addEventListener('submit', async (e) => {
        e.preventDefault();
        emailError.textContent = '';
        passwordError.textContent = '';

        const email = form.email.value;
        const password = form.password.value;

        try {
            const res = await fetch("login", {
                method: "POST",
                body: JSON.stringify({ email, password }),
                headers: {
                    "Content-Type": "application/json"
                }
            });

            const data = await res.json();
            console.log(data);
            if (data.errors) {
                emailError.textContent = data.errors.email;
                passwordError.textContent = data.errors.password;
                console.log(emailError.textContent);
                console.log(passwordError.textContent);
            }
            if(data.user){
                location.assign('/url');
            }

        } catch (error) {
            console.log(error);
        }
    })