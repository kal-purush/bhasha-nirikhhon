const express = require('express');
const cors = require('cors');
const multer = require('multer');
const path = require('path');
const mysql = require('mysql2');

const app = express();
const PORT = 3000;

// Middleware
app.use(cors());
app.use(express.json());
app.use('/uploads', express.static(path.join(__dirname, 'uploads')));

// MySQL connection
const db = mysql.createConnection({
  host: 'localhost',
  user: 'root',
  password: '', // change if needed
  database: 'sk_pizza'
});

db.connect(err => {
  if (err) {
    console.error('MySQL connection failed:', err);
    return;
  }
  console.log('Connected to MySQL');
});

// Multer setup for image uploads
const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    cb(null, './uploads/');
  },
  filename: (req, file, cb) => {
    cb(null, `${Date.now()}-${file.originalname}`);
  }
});
const upload = multer({ storage });

// ------------------ PRODUCTS ROUTES ------------------

// Get all products
app.get('/api/products', (req, res) => {
  db.query('SELECT * FROM products', (err, results) => {
    if (err) return res.status(500).json({ error: err.message });
    res.json(results);
  });
});

// Add new product
app.post('/api/products', upload.single('image'), (req, res) => {
  const { name, price } = req.body;
  const image = `/uploads/${req.file.filename}`;

  const sql = 'INSERT INTO products (name, price, image) VALUES (?, ?, ?)';
  db.query(sql, [name, price, image], (err, result) => {
    if (err) return res.status(500).json({ error: err.message });

    res.json({ id: result.insertId, name, price, image });
  });
});

// Delete a product
app.delete('/api/products/:id', (req, res) => {
  const { id } = req.params;
  db.query('DELETE FROM products WHERE id = ?', [id], (err, result) => {
    if (err) return res.status(500).json({ error: err.message });
    res.json({ success: true });
  });
});

// ------------------ ORDERS ROUTES ------------------

// Get all orders
app.get('/api/orders', (req, res) => {
  const sql = 'SELECT * FROM orders';
  db.query(sql, (err, orders) => {
    if (err) return res.status(500).json({ error: err.message });

    const customerSql = 'SELECT * FROM customers';
    db.query(customerSql, (err, customers) => {
      if (err) return res.status(500).json({ error: err.message });

      const itemSql = 'SELECT * FROM order_items';
      db.query(itemSql, (err, items) => {
        if (err) return res.status(500).json({ error: err.message });

        // Map customers and items to orders
        const formatted = orders.map(order => {
          const customer = customers.find(c => c.id === order.customer_id);
          const orderItems = items
            .filter(i => i.order_id === order.id)
            .map(i => ({ name: i.item_name, quantity: i.quantity }));

          return {
            id: order.id,
            orderNumber: order.order_number,
            customer: {
              name: customer?.name || 'Unknown',
              address: customer?.address || '',
              phone: customer?.phone || ''
            },
            items: orderItems,
            total: order.total,
            status: order.status
          };
        });

        res.json(formatted);
      });
    });
  });
});

// Update order status
app.put('/api/orders/:id', (req, res) => {
  const { id } = req.params;
  const { status } = req.body;

  const sql = 'UPDATE orders SET status = ? WHERE id = ?';
  db.query(sql, [status, id], (err) => {
    if (err) return res.status(500).json({ error: err.message });
    res.json({ success: true });
  });
});

// ------------------ START SERVER ------------------

app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});