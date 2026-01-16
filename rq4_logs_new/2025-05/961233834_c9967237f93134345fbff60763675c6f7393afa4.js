fetch("http://localhost:3000/historico")
    .then(res => res.json())
    .then(data => {
        console.log("Dados do histórico:", data);  
        const container = document.getElementById("historico");
        container.innerHTML = "<h2>Histórico de Empréstimos</h2>";

        data.forEach(item => {
            console.log("ID do empréstimo:", item.id);//Verifique se o id está sendo capturado

            const div = document.createElement("div");
            div.classList.add("registro");

            div.innerHTML = `
                <strong>Usuário:</strong> ${item.usuario} <br>
                <strong>Livro:</strong> ${item.titulo} <br>
                <strong>Data:</strong> ${new Date(item.data_emprestimo).toLocaleDateString("pt-BR")}
                ${new Date(item.data_emprestimo).toLocaleTimeString("pt-BR")} <br>
                <button class="btn-devolver" data-id="${item.id}">Devolver</button>
                <hr>
            `;

            container.appendChild(div);
        });

        //Adiciona eventos aos botões de devolução
        document.querySelectorAll(".btn-devolver").forEach(button => {
            button.addEventListener("click", () => {
                const id = button.getAttribute("data-id");
                console.log("ID do empréstimo a ser devolvido:", id);//Verifique ve se o id está sendo capturado
                if (!id) {
                    alert("ID inválido para devolução!");
                    return;
                }

                fetch("http://localhost:3000/devolver", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ id: Number(id) })
                })
                    .then(res => res.json())
                    .then(data => {
                        console.log("Resposta do servidor:", data);
                        if (data.message === "Livro devolvido com sucesso!") {
                            button.disabled = true;
                            button.textContent = "Devolvido";
                        }
                        alert(data.message); //Mensagens de sucesso ou erro
                    })
                    .catch(err => {
                        console.error("Erro ao devolver:", err);
                        alert("Erro ao comunicar com o servidor.");
                    });
            });
        });
    })
    .catch(err => {
        console.error("Erro ao carregar histórico:", err);
    });