/*const getOptionChart = async () => {
    try {
        const response = await fetch("http://127.0.0.1:8000/entrada/get_chart/");
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        return await response.json();
    } catch (ex) {
        console.error("Error fetching chart data:", ex);
        // Puedes mostrar un mensaje de error en el DOM o hacer algo más aquí
    }
};


const initChart = async () => {
    const myChart = echarts.init(document.getElementById("chart"));

    myChart.setOption(await getOptionChart());

    myChart.resize();
};

window.addEventListener("load", async () => {
    await initChart();
    setInterval(async () => {
        await initChart();
    }, 2000);
});*/
const getChartData = async (anio = null) => {
    try {
        let url = "http://127.0.0.1:8000/entrada/obtener-datos-para-grafico/";
        if (anio) {
            url += `${anio}/`;
        }

        const response = await fetch(url);
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }

        const data = await response.json();
        const chart = echarts.init(document.getElementById("chart"));

        // Obtén los meses y los totales del objeto de respuesta JSON
        const meses = data.resultados.map(item => item.Mes);
        const totales = data.resultados.map(item => item.Total);

        const option = {
            tooltip: {
                show: true,
                trigger: "axis",
                triggerOn: "mousemove|click"
            },
            xAxis: [
                {
                    type: "category",
                    data: meses.map(mes => mesesTexto[mes - 1]) // mesesTexto es un array con los nombres de los meses en orden
                }
            ],
            yAxis: [
                {
                    type: "value"
                }
            ],
            series: [
                {
                    data: totales,
                    type: "line",
                    itemStyle: {
                        color: "blue" // Puedes cambiar el color si lo deseas
                    },
                    lineStyle: {
                        color: "blue" // Puedes cambiar el color si lo deseas
                    }
                }
            ]
        };

        chart.setOption(option);
        chart.resize();
    } catch (ex) {
        console.error("Error fetching chart data:", ex);
        // Manejar el error según sea necesario
    }
};

// ...

// Define un array con los nombres de los meses en orden
const mesesTexto = ["Ene", "Feb", "Mar", "Abr", "May", "Jun", "Jul", "Ago", "Sep", "Oct", "Nov", "Dic"];

window.addEventListener("load", async () => {
    await llenarCombobox();
    const comboboxAnios = document.getElementById("combobox-anios");
    comboboxAnios.addEventListener("change", () => {
        const anioSeleccionado = comboboxAnios.value;
        getChartData(anioSeleccionado);
    });
    getChartData(); // Para cargar el gráfico inicial sin un año seleccionado
});


const llenarCombobox = async () => {
    try {
        const response = await fetch("http://127.0.0.1:8000/entrada/obtener-anios/");
        if (!response.ok) {
            throw new Error(`HTTP error! status: ${response.status}`);
        }
        const data = await response.json();

        const comboboxAnios = document.getElementById("combobox-anios");
        comboboxAnios.innerHTML = ''; // Limpiar el combobox antes de llenarlo

        // Utiliza un Set para almacenar años únicos y evitar duplicados
        const uniqueAnios = new Set(data.anios);

        // Llena el combobox con los años únicos obtenidos
        uniqueAnios.forEach(anio => {
            const option = document.createElement("option");
            option.value = anio;
            option.text = anio;
            comboboxAnios.appendChild(option);
        });
    } catch (ex) {
        console.error("Error fetching years:", ex);
        
    }
};

window.addEventListener("load", async () => {
    await llenarCombobox();
    const comboboxAnios = document.getElementById("combobox-anios");
    comboboxAnios.addEventListener("change", () => {
        const anioSeleccionado = comboboxAnios.value;
        getChartData(anioSeleccionado);
    });
    getChartData(); // Para cargar el gráfico inicial sin un año seleccionado
});
