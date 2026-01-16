// Cart

/**
 * @swagger
 * /api/v1/cart:
 *   get:
 *     summary: Retrieve cart items for the authenticated user
 *     tags: [🛒 Cart]
 *     responses:
 *       200:
 *         description: Cart items successfully retrieved
 *         content:
 *           application/json:
 *             example:
 *               status: "success"
 *               data:
 *                 cartItems:
 *                   - productId: "12345"
 *                     quantity: 2
 *                     price: 25.99
 *                     # ... (other cart item details)
 *       404:
 *         description: No cart items found for the authenticated user
 *         content:
 *           application/json:
 *             example:
 *               status: "error"
 *               message: "No cart items found for the user"
 *       500:
 *         description: Internal server error
 *         content:
 *           application/json:
 *             example:
 *               status: "error"
 *               message: "Internal server error"
 */

/**
 * @swagger
 * /api/v1/cart:
 *   post:
 *     summary: Add a product to the cart for the authenticated user
 *     tags: [🛒 Cart]
 *     security:
 *       - bearerAuth: []
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               productId:
 *                 type: string
 *               quantity:
 *                 type: number
 *             example:
 *               productId: "12345"
 *               quantity: 2
 *     responses:
 *       201:
 *         description: Product successfully added to the cart
 *         content:
 *           application/json:
 *             example:
 *               status: "success"
 *               data:
 *                 cartItem:
 *                   productId: "12345"
 *                   quantity: 2
 *                   price: 25.99
 *                   # ... (other cart item details)
 *       400:
 *         description: Bad request. Validation errors in the request body.
 *         content:
 *           application/json:
 *             example:
 *               status: "error"
 *               message: "Invalid request body"
 *       500:
 *         description: Internal server error
 *         content:
 *           application/json:
 *             example:
 *               status: "error"
 *               message: "Internal server error"
 */

/**
 * @swagger
 * /api/v1/cart/{id}:
 *   patch:
 *     summary: Update the quantity of a product in the cart by ID for the authenticated user
 *     tags: [🛒 Cart]
 *     security:
 *       - bearerAuth: []
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: string
 *         description: ID of the cart item to be updated
 *     requestBody:
 *       description: Updated cart item details
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               quantity:
 *                 type: number
 *             example:
 *               quantity: 3
 *     responses:
 *       200:
 *         description: Cart item quantity successfully updated
 *         content:
 *           application/json:
 *             example:
 *               status: "success"
 *               message: "Cart item quantity updated successfully"
 *               data:
 *                 updatedCartItem:
 *                   productId: "12345"
 *                   quantity: 3
 *                   price: 25.99
 *                   # ... (other updated cart item details)
 *       400:
 *         description: Bad request. Invalid cart item ID or validation errors.
 *         content:
 *           application/json:
 *             example:
 *               status: "error"
 *               message: "Invalid cart item ID"
 *       500:
 *         description: Internal server error
 *         content:
 *           application/json:
 *             example:
 *               status: "error"
 *               message: "Internal server error"
 */
/**
 * @swagger
 * /api/v1/cart/{id}:
 *   delete:
 *     summary: Remove a product from the cart by ID for the authenticated user
 *     tags: [🛒 Cart]
 *     security:
 *       - bearerAuth: []
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: string
 *         description: ID of the cart item to be deleted
 *     responses:
 *       200:
 *         description: Product successfully removed from the cart
 *         content:
 *           application/json:
 *             example:
 *               status: "success"
 *               message: "Product removed from the cart successfully"
 *               data:
 *                 deletedCartItem:
 *                   productId: "12345"
 *                   quantity: 2
 *                   price: 25.99
 *                   # ... (other deleted cart item details)
 *       400:
 *         description: Bad request. Invalid cart item ID or validation errors.
 *         content:
 *           application/json:
 *             example:
 *               status: "error"
 *               message: "Invalid cart item ID"
 *       500:
 *         description: Internal server error
 *         content:
 *           application/json:
 *             example:
 *               status: "error"
 *               message: "Internal server error"
 */

// User
/**
 * @swagger
 * /api/v1/users/own:
 *   get:
 *     summary: Fetch a user by ID
 *     tags: [👤 Users]
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: string
 *         description: ID of the user to fetch
 *     responses:
 *       200:
 *         description: The fetched user
 *         content:
 *           application/json:
 *             example:
 *               id: "65870c4cfdff5cfa6ef4687a"
 *               addresses: []
 *               email: "abcd@gmail.com"
 *               role: "user"
 *       400:
 *         description: Bad request. Invalid user ID.
 *         content:
 *           application/json:
 *             example:
 *               message: "Invalid user ID"
 *       404:
 *         description: User not found
 *         content:
 *           application/json:
 *             example:
 *               message: "User not found"
 *       500:
 *         description: Internal server error
 *         content:
 *           application/json:
 *             example:
 *               message: "Internal server error"
 */

/**
 * @swagger
 * /api/v1/users/{id}:
 *   patch:
 *     summary: Update a user by ID
 *     tags: [👤 Users]
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: string
 *         description: ID of the user to update
 *     requestBody:
 *       description: Updated user details
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               email:
 *                 type: string
 *               password:
 *                 type: object
 *                 properties:
 *                   type:
 *                     type: string
 *                   data:
 *                     type: array
 *                     items:
 *                       type: number
 *               role:
 *                 type: string
 *               addresses:
 *                 type: array
 *               orders:
 *                 type: array
 *               salt:
 *                 type: object
 *                 properties:
 *                   type:
 *                     type: string
 *                   data:
 *                     type: array
 *                     items:
 *                       type: number
 *               id:
 *                 type: string
 *             example:
 *               email: "abcd@gmail.com"
 *               password: { "type": "Buffer", "data": [80, 147, ...] }
 *               role: "user"
 *               addresses: []
 *               orders: []
 *               salt: { "type": "Buffer", "data": [162, 242, ...] }
 *               id: "65870c4cfdff5cfa6ef4687a"
 *     responses:
 *       200:
 *         description: The updated user
 *         content:
 *           application/json:
 *             example:
 *               id: "65870c4cfdff5cfa6ef4687a"
 *               email: "abcd@gmail.com"
 *               role: "user"
 *               addresses: []
 *               orders: []
 *               salt: { "type": "Buffer", "data": [162, 242, ...] }
 *       400:
 *         description: Bad request. Invalid user ID or request body.
 *         content:
 *           application/json:
 *             example:
 *               message: "Invalid user ID"
 *       404:
 *         description: User not found
 *         content:
 *           application/json:
 *             example:
 *               message: "User not found"
 *       500:
 *         description: Internal server error
 *         content:
 *           application/json:
 *             example:
 *               message: "Internal server error"
 */

//Product
/**
 * @swagger
 * /api/v1/products:
 *   post:
 *     summary: Create a new product
 *     tags: [🧺 Products]
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           example:
 *             colors: []
 *             sizes: []
 *             deleted: false
 *             title: "New"
 *             description: "Comfortable stretch cloth, lightweight body; ,rubber sole, anti-skid wear;"
 *             price: 20
 *             discountPercentage: 8.71
 *             rating: 4.33
 *             stock: 137
 *             brand: "Sneakers"
 *             category: "mens-shoes"
 *             thumbnail: "https://i.dummyjson.com/data/products/59/thumbnail.jpg"
 *             images:
 *               - "https://i.dummyjson.com/data/products/59/1.jpg"
 *               - "https://i.dummyjson.com/data/products/59/2.jpg"
 *               - "https://i.dummyjson.com/data/products/59/3.jpg"
 *               - "https://i.dummyjson.com/data/products/59/4.jpg"
 *               - "https://i.dummyjson.com/data/products/59/thumbnail.jpg"
 *     responses:
 *       201:
 *         description: The product was successfully created
 *         content:
 *           application/json:
 *             example:
 *               message: "Product created successfully"
 *               savedProduct:
 *                 colors: []
 *                 sizes: []
 *                 deleted: false
 *                 title: "New"
 *                 description: "Comfortable stretch cloth, lightweight body; ,rubber sole, anti-skid wear;"
 *                 price: 20
 *                 discountPercentage: 8.71
 *                 rating: 4.33
 *                 stock: 137
 *                 brand: "Sneakers"
 *                 category: "mens-shoes"
 *                 thumbnail: "https://i.dummyjson.com/data/products/59/thumbnail.jpg"
 *                 images:
 *                   - "https://i.dummyjson.com/data/products/59/1.jpg"
 *                   - "https://i.dummyjson.com/data/products/59/2.jpg"
 *                   - "https://i.dummyjson.com/data/products/59/3.jpg"
 *                   - "https://i.dummyjson.com/data/products/59/4.jpg"
 *                   - "https://i.dummyjson.com/data/products/59/thumbnail.jpg"
 *       400:
 *         description: Please fill all required fields
 *         content:
 *           application/json:
 *             example:
 *               message: "Please fill all required fields"
 *       500:
 *         description: Internal server error
 *         content:
 *           application/json:
 *             example:
 *               message: "Internal server error"
 */

/**
 * @swagger
 * /api/v1/products:
 *   get:
 *     summary: Retrieve a list of products with optional filtering, sorting, pagination, and search
 *     tags: [🧺 Products]
 *     parameters:
 *       - in: query
 *         name: admin
 *         schema:
 *           type: string
 *         description: Flag to include deleted products (optional)
 *       - in: query
 *         name: category
 *         schema:
 *           type: string
 *         description: Filter products by category (optional)
 *       - in: query
 *         name: brand
 *         schema:
 *           type: string
 *         description: Filter products by brand (optional)
 *       - in: query
 *         name: _sort
 *         schema:
 *           type: string
 *         description: Sort products by a specified attribute (optional)
 *       - in: query
 *         name: _order
 *         schema:
 *           type: string
 *         description: Specify the sorting order (asc or desc) (optional)
 *       - in: query
 *         name: _page
 *         schema:
 *           type: string
 *         description: Current page for pagination (optional)
 *       - in: query
 *         name: _limit
 *         schema:
 *           type: string
 *         description: Number of items per page for pagination (optional)
 *       - in: query
 *         name: q
 *         schema:
 *           type: string
 *         description: Search query to filter products (optional)
 *     responses:
 *       200:
 *         description: A list of products with optional filtering, sorting, and pagination
 *         content:
 *           application/json:
 *             examples:
 *               multipleExamples:
 *                 summary: Multiple Examples of Product Listing
 *                 value:
 *                   - colors: []
 *                     sizes: []
 *                     deleted: false
 *                     id: "657483b37ebcb47acc9407b2"
 *                     title: "Samsung Universe 9"
 *                     description: "Samsung's new variant which goes beyond Galaxy to the Universe"
 *                     price: 1249
 *                     discountPercentage: 15.46
 *                     rating: 4.09
 *                     stock: 36
 *                     brand: "Samsung"
 *                     category: "smartphones"
 *                     thumbnail: "https://i.dummyjson.com/data/products/3/thumbnail.jpg"
 *                     images:
 *                       - "https://i.dummyjson.com/data/products/3/1.jpg"
 *                   - ... (remaining products)
 *               specificExample:
 *                 summary: Example with specific parameters
 *                 value:
 *                   - colors: []
 *                     sizes: []
 *                     deleted: false
 *                     id: "exampleProductId1"
 *                     title: "Fragrance Product 1"
 *                     description: "A delightful fragrance"
 *                     price: 20
 *                     discountPercentage: 5.0
 *                     rating: 4.5
 *                     stock: 50
 *                     brand: "Samsung"
 *                     category: "fragrances"
 *                     thumbnail: "https://example.com/thumbnail.jpg"
 *                     images:
 *                       - "https://example.com/image1.jpg"
 *                       - "https://example.com/image2.jpg"
 *                   - ... (remaining products)
 *       404:
 *         description: No products found
 *         content:
 *           application/json:
 *             example:
 *               message: "No products found"
 *       500:
 *         description: Internal server error
 *         content:
 *           application/json:
 *             example:
 *               message: "Internal server error"
 */

/**
 * @swagger
 * /api/v1/products/{id}:
 *   get:
 *     summary: Retrieve a single product by ID
 *     tags: [🧺 Products]
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: string
 *         description: ID of the product to retrieve
 *     responses:
 *       200:
 *         description: The product with the specified ID
 *         content:
 *           application/json:
 *             example:
 *               id: "productID123"
 *               title: "Product 1"
 *               description: "Description for Product 1"
 *               price: 19.99
 *               category: "Electronics"
 *               brand: "Brand X"
 *               # ... (other product details)
 *       404:
 *         description: Product not found
 *         content:
 *           application/json:
 *             example:
 *               message: "Product not found"
 *       500:
 *         description: Internal server error
 *         content:
 *           application/json:
 *             example:
 *               message: "Internal server error"
 */
/**
 * @swagger
 * /api/v1/products/{id}:
 *   patch:
 *     summary: Update a product by ID
 *     tags: [🧺 Products]
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: string
 *         description: ID of the product to update
 *     requestBody:
 *       description: Updated product details
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               title:
 *                 type: string
 *               description:
 *                 type: string
 *               price:
 *                 type: number
 *               discountPercentage:
 *                 type: number
 *             example:
 *               title: "Updated Product"
 *               description: "Updated description"
 *               price: 29.99
 *               discountPercentage: 10
 *     responses:
 *       200:
 *         description: The updated product
 *         content:
 *           application/json:
 *             example:
 *               id: "updatedProductID123"
 *               title: "Updated Product"
 *               description: "Updated description"
 *               price: 29.99
 *               discountPercentage: 10
 *               # ... (other updated product details)
 *       400:
 *         description: Bad request. Invalid product ID or request body.
 *         content:
 *           application/json:
 *             example:
 *               message: "Invalid product ID"
 *       404:
 *         description: Product not found
 *         content:
 *           application/json:
 *             example:
 *               message: "Product not found"
 *       500:
 *         description: Internal server error
 *         content:
 *           application/json:
 *             example:
 *               message: "Internal server error"
 */
//Category

/**
 * @swagger
 * /api/v1/createCategory:
 *   post:
 *     summary: Create a new category
 *     tags: [🧩 Categories]
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           example:
 *             value: "New Category"
 *             description: "A description for the new category"
 *     responses:
 *       201:
 *         description: Category created successfully
 *         content:
 *           application/json:
 *             example:
 *               message: "Category created successfully"
 *               savedCategory:
 *                 value: "New Category"
 *                 description: "A description for the new category"
 *       400:
 *         description: Bad request, validation errors
 *         content:
 *           application/json:
 *             example:
 *               errors: ["Validation error 1", "Validation error 2"]
 *       500:
 *         description: Internal server error
 *         content:
 *           application/json:
 *             example:
 *               message: "Internal server error"
 */
/**
 * @swagger
 * /api/v1/fetchCategory:
 *   get:
 *     summary: Retrieve a list of all categories
 *     tags: [🧩 Categories]
 *     responses:
 *       200:
 *         description: A list of all categories
 *         content:
 *           application/json:
 *             example:
 *               - value: "Category 1"
 *                 description: "Description for Category 1"
 *               - value: "Category 2"
 *                 description: "Description for Category 2"
 *               # ... (remaining categories)
 *       404:
 *         description: No categories found
 *         content:
 *           application/json:
 *             example:
 *               message: "No categories found"
 *       500:
 *         description: Internal server error
 *         content:
 *           application/json:
 *             example:
 *               message: "Internal server error"
 */

//Brand
/**
 * @swagger
 * /api/v1/brands:
 *   post:
 *     summary: Create a new brand
 *     tags: [🧠 Brands]
 *     requestBody:
 *       description: Brand details to be created
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               value:
 *                 type: string
 *               label:
 *                 type: string
 *             example:
 *               value: "New Brand"
 *               label: "A description for the new Brand"
 *     responses:
 *       201:
 *         description: Brand created successfully
 *         content:
 *           application/json:
 *             example:
 *               message: "Brand created successfully"
 *               savedBrand:
 *                 value: "New Brand"
 *                 label: "A description for the new Brand"
 *       400:
 *         description: Bad request. Validation errors in the request body.
 *         content:
 *           application/json:
 *             example:
 *               errors: [
 *                 {
 *                   value: "",
 *                   msg: "Brand value is required",
 *                   param: "value",
 *                   location: "body"
 *                 },
 *                 {
 *                   value: "",
 *                   msg: "Brand label is required",
 *                   param: "label",
 *                   location: "body"
 *                 }
 *               ]
 *       500:
 *         description: Internal server error
 *         content:
 *           application/json:
 *             example:
 *               message: "Internal server error"
 */
/**
 * @swagger
 * /api/v1/brands:
 *   get:
 *     summary: Retrieve a list of all brands
 *     tags: [🧠 Brands]
 *     responses:
 *       200:
 *         description: A list of all brands
 *         content:
 *           application/json:
 *             example:
 *               - value: "New Brand"
 *                 label: "A description for the new Brand"
 *               - value: "Another Brand"
 *                 label: "Another Label"
 *               # ... (remaining brands)
 *       404:
 *         description: No brands found
 *         content:
 *           application/json:
 *             example:
 *               message: "No brands found"
 *       500:
 *         description: Internal server error
 *         content:
 *           application/json:
 *             example:
 *               message: "Internal server error"
 */
// Todo: Create a new order with Email
/* ☑️ Create a new order ☑️ */

/**
 * @swagger
 * /api/v1/orders:
 *   post:
 *     summary: Create a new order
 *     tags: [📦 Orders]
 *     requestBody:
 *       required: true
 *       content:
 *         application/json:
 *           example:
 *             items:
 *               -
 *                 quantity: 1
 *                 product:
 *                   id: "657483b37ebcb47acc9407c8"
 *             totalAmount: 60.49
 *             totalItems: 1
 *             user:
 *               id: "65b618b5e9a9eb06fc5c5416"
 *             paymentMethod: "cash"
 *             paymentStatus: "pending"
 *             status: "pending"
 *             selectedAddress:
 *               name: "Subham Maity"
 *               email: "maitysubham4041@gmail.com"
 *               phone: "9933515901"
 *               street: "kolkata"
 *               city: "kolkata"
 *               state: "wb"
 *               pinCode: "700041"
 *     responses:
 *       201:
 *         description: The order was successfully created
 *         content:
 *           application/json:
 *             example:
 *               items:
 *                 -
 *                   quantity: 1
 *                   product:
 *                     id: "657483b37ebcb47acc9407c8"
 *               totalAmount: 60.49
 *               totalItems: 1
 *               user:
 *                 id: "65b618b5e9a9eb06fc5c5416"
 *               paymentMethod: "cash"
 *               paymentStatus: "pending"
 *               status: "pending"
 *               selectedAddress:
 *                 name: "Subham Maity"
 *                 email: "maitysubham4041@gmail.com"
 *                 phone: "9933515901"
 *                 street: "kolkata"
 *                 city: "kolkata"
 *                 state: "wb"
 *                 pinCode: "700041"
 *       500:
 *         description: Internal server error
 *         content:
 *           application/json:
 *             example:
 *               message: "Internal server error"
 */
// Todo: Fetch All Orders
/* ☑️ FETCH ALL ORDERS ☑️ */

/**
 * @swagger
 * /api/v1/orders/own:
 *   get:
 *     summary: Fetch all orders for the authenticated user
 *     tags: [📦 Orders]
 *     security:
 *       - bearerAuth: []
 *     responses:
 *       200:
 *         description: A list of orders for the authenticated user
 *         content:
 *           application/json:
 *             examples:
 *               multipleExamples:
 *                 summary: Multiple Examples of User's Orders
 *                 value:
 *                   -
 *                     items:
 *                       -
 *                         quantity: 1
 *                         product:
 *                           id: "657483b37ebcb47acc9407c8"
 *                     totalAmount: 60.49
 *                     totalItems: 1
 *                     user:
 *                       id: "65b618b5e9a9eb06fc5c5416"
 *                     paymentMethod: "cash"
 *                     paymentStatus: "pending"
 *                     status: "pending"
 *                     selectedAddress:
 *                       name: "Subham Maity"
 *                       email: "maitysubham4041@gmail.com"
 *                       phone: "9933515901"
 *                       street: "kolkata"
 *                       city: "kolkata"
 *                       state: "wb"
 *                       pinCode: "700041"
 *                   - ... (remaining orders)
 *               specificExample:
 *                 summary: Example of User's Order
 *                 value:
 *                   -
 *                     items:
 *                       -
 *                         quantity: 1
 *                         product:
 *                           id: "657483b37ebcb47acc9407c8"
 *                     totalAmount: 60.49
 *                     totalItems: 1
 *                     user:
 *                       id: "65b618b5e9a9eb06fc5c5416"
 *                     paymentMethod: "cash"
 *                     paymentStatus: "pending"
 *                     status: "pending"
 *                     selectedAddress:
 *                       name: "Subham Maity"
 *                       email: "maitysubham4041@gmail.com"
 *                       phone: "9933515901"
 *                       street: "kolkata"
 *                       city: "kolkata"
 *                       state: "wb"
 *                       pinCode: "700041"
 *       500:
 *         description: Internal server error
 *         content:
 *           application/json:
 *             example:
 *               message: "Internal server error"
 */
// Todo: Delete an Order
/* ☑️ Delete an order ☑️ */

/**
 * @swagger
 * /api/v1/orders/{id}:
 *   delete:
 *     summary: Delete an order by ID
 *     tags: [📦 Orders]
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: string
 *         description: ID of the order to delete
 *     responses:
 *       200:
 *         description: The order was successfully deleted
 *         content:
 *           application/json:
 *             example:
 *               items:
 *                 -
 *                   quantity: 1
 *                   product:
 *                     id: "657483b37ebcb47acc9407c8"
 *               totalAmount: 60.49
 *               totalItems: 1
 *               user:
 *                 id: "65b618b5e9a9eb06fc5c5416"
 *               paymentMethod: "cash"
 *               paymentStatus: "pending"
 *               status: "pending"
 *               selectedAddress:
 *                 name: "Subham Maity"
 *                 email: "maitysubham4041@gmail.com"
 *                 phone: "9933515901"
 *                 street: "kolkata"
 *                 city: "kolkata"
 *                 state: "wb"
 *                 pinCode: "700041"
 *       404:
 *         description: Order not found
 *         content:
 *           application/json:
 *             example:
 *               message: "Order not found"
 *       500:
 *         description: Internal server error
 *         content:
 *           application/json:
 *             example:
 *               message: "Internal server error"
 */

// Todo: Update an Order
/* ☑️ Update an order ☑️ */

/**
 * @swagger
 * /api/v1/orders/{id}:
 *   patch:
 *     summary: Update an order by ID
 *     tags: [📦 Orders]
 *     parameters:
 *       - in: path
 *         name: id
 *         required: true
 *         schema:
 *           type: string
 *         description: ID of the order to update
 *     requestBody:
 *       description: Updated order details
 *       required: true
 *       content:
 *         application/json:
 *           schema:
 *             type: object
 *             properties:
 *               items:
 *                 type: array
 *                 items:
 *                   type: object
 *                   properties:
 *                     quantity:
 *                       type: number
 *                     product:
 *                       type: object
 *                       properties:
 *                         id:
 *                           type: string
 *               totalAmount:
 *                 type: number
 *               totalItems:
 *                 type: number
 *               user:
 *                 type: object
 *                 properties:
 *                   id:
 *                     type: string
 *               paymentMethod:
 *                 type: string
 *               paymentStatus:
 *                 type: string
 *               status:
 *                 type: string
 *               selectedAddress:
 *                 type: object
 *                 properties:
 *                   name:
 *                     type: string
 *                   email:
 *                     type: string
 *                   phone:
 *                     type: string
 *                   street:
 *                     type: string
 *                   city:
 *                     type: string
 *                   state:
 *                     type: string
 *                   pinCode:
 *                     type: string
 *     responses:
 *       200:
 *         description: The updated order
 *         content:
 *           application/json:
 *             example:
 *               items:
 *                 -
 *                   quantity: 1
 *                   product:
 *                     id: "657483b37ebcb47acc9407c8"
 *               totalAmount: 60.49
 *               totalItems: 1
 *               user:
 *                 id: "65b618b5e9a9eb06fc5c5416"
 *               paymentMethod: "cash"
 *               paymentStatus: "pending"
 *               status: "pending"
 *               selectedAddress:
 *                 name: "Subham Maity"
 *                 email: "maitysubham4041@gmail.com"
 *                 phone: "9933515901"
 *                 street: "kolkata"
 *                 city: "kolkata"
 *                 state: "wb"
 *                 pinCode: "700041"
 *       400:
 *         description: Bad request. Invalid order ID or request body.
 *         content:
 *           application/json:
 *             example:
 *               message: "Invalid order ID"
 *       404:
 *         description: Order not found
 *         content:
 *           application/json:
 *             example:
 *               message: "Order not found"
 *       500:
 *         description: Internal server error
 *         content:
 *           application/json:
 *             example:
 *               message: "Internal server error"
 */
/* ☑️ Fetch all orders with optional sorting and pagination ☑️ */

/**
 * @swagger
 * /api/v1/orders:
 *   get:
 *     summary: Fetch all orders with optional sorting and pagination
 *     tags: [📦 Orders]
 *     parameters:
 *       - in: query
 *         name: _sort
 *         schema:
 *           type: string
 *         description: Sort orders by a specified attribute (optional)
 *       - in: query
 *         name: _order
 *         schema:
 *           type: string
 *         description: Specify the sorting order (asc or desc) (optional)
 *       - in: query
 *         name: _page
 *         schema:
 *           type: string
 *         description: Current page for pagination (optional)
 *       - in: query
 *         name: _limit
 *         schema:
 *           type: string
 *         description: Number of items per page for pagination (optional)
 *     responses:
 *       200:
 *         description: A list of orders with optional sorting and pagination
 *         content:
 *           application/json:
 *             examples:
 *               multipleExamples:
 *                 summary: Multiple Examples of Orders Listing
 *                 value:
 *                   -
 *                     items:
 *                       -
 *                         quantity: 1
 *                         product:
 *                           id: "657483b37ebcb47acc9407c8"
 *                     totalAmount: 60.49
 *                     totalItems: 1
 *                     user:
 *                       id: "65b618b5e9a9eb06fc5c5416"
 *                     paymentMethod: "cash"
 *                     paymentStatus: "pending"
 *                     status: "pending"
 *                     selectedAddress:
 *                       name: "Subham Maity"
 *                       email: "maitysubham4041@gmail.com"
 *                       phone: "9933515901"
 *                       street: "kolkata"
 *                       city: "kolkata"
 *                       state: "wb"
 *                       pinCode: "700041"
 *                   - ... (remaining orders)
 *               specificExample:
 *                 summary: Example with specific parameters
 *                 value:
 *                   -
 *                     items:
 *                       -
 *                         quantity: 1
 *                         product:
 *                           id: "657483b37ebcb47acc9407c8"
 *                     totalAmount: 60.49
 *                     totalItems: 1
 *                     user:
 *                       id: "65b618b5e9a9eb06fc5c5416"
 *                     paymentMethod: "cash"
 *                     paymentStatus: "pending"
 *                     status: "pending"
 *                     selectedAddress:
 *                       name: "Subham Maity"
 *                       email: "maitysubham4041@gmail.com"
 *                       phone: "9933515901"
 *                       street: "kolkata"
 *                       city: "kolkata"
 *                       state: "wb"
 *                       pinCode: "700041"
 *                   - ... (remaining orders)
 *       500:
 *         description: Internal server error
 *         content:
 *           application/json:
 *             example:
 *               message: "Internal server error"
 */