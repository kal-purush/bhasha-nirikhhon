class Products {
    async getProducts() {
        try {
            let result = await fetch('js/products.json')

            let data = await result.json();

            let products = data.items;

            // get all details produts from json
            products = products.map(item => {
                const { title, price } = item.fields;
                const { id } = item.sys;
                const image = item.fields.image.fields.file.url;
                return { title, price, id, image }
            });

            return products;

        } catch (error) {
            console.log(error);
        }

    }
}