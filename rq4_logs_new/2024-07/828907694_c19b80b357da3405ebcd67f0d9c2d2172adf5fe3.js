// ------------  for local development --------------
const express = require('express');
const bodyParser = require('body-parser');
const mysql = require('mysql');
const cors = require('cors');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const multer = require('multer');
const path = require('path');
const fs = require('fs');

const app = express();
const router = express.Router();

app.use(bodyParser.json());
app.use(cors());



const db = mysql.createConnection({
  host: 'localhost',
  user: 'root',
  password: '',
  database: 'alaqariyya',
  charset: 'utf8mb4'
});

db.connect(err => {
  if (err) throw err;
  console.log('MySQL Connected...');
});

const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    const uploadPath = path.join(__dirname, 'uploads');
    if (!fs.existsSync(uploadPath)) {
      fs.mkdirSync(uploadPath);
    }
    cb(null, 'uploads');
  },
  filename: (req, file, cb) => {
    cb(null, Date.now() + path.extname(file.originalname));
  }
});

const upload = multer({ storage });

app.use('/uploads', express.static(path.join(__dirname, 'uploads')));

app.get('/', (req, res) => {
  res.send('Welcome to the Node.js backend for ALAQARIYYA');
});


app.get('/properties', (req, res) => {
  const { type, location, page = 1, limit = 8, lang = 'en' } = req.query;
  const offset = (page - 1) * limit;
  const titleColumn = `title_${lang}` in req.query ? `title_${lang}` : `title_ar`;
  const descriptionColumn = `description_${lang}` in req.query ? `description_${lang}` : `description_ar`;

  let sql = `
    SELECT p.property_id, p.${titleColumn} as title, p.price, p.location, p.bedrooms, p.bathrooms, p.salon, p.area, p.type, p.available, p.floors, p.availability_date, pi.image_url, p.${descriptionColumn} as description
    FROM properties p
    LEFT JOIN (
      SELECT property_id, MIN(image_url) as image_url
      FROM property_images
      GROUP BY property_id
    ) pi ON p.property_id = pi.property_id
  `;
  const params = [];

  if (type && type !== 'all') {
    sql += ' WHERE p.type = ?';
    params.push(type);
  }

  if (location) {
    sql += params.length ? ' AND p.location LIKE ?' : ' WHERE p.location LIKE ?';
    params.push(`%${location}%`);
  }

  sql += ' LIMIT ? OFFSET ?';
  params.push(parseInt(limit), parseInt(offset));

  db.query(sql, params, (err, result) => {
    if (err) {
      console.error('Error querying properties:', err);
      return res.status(500).send('Database query error');
    }

    let countSql = 'SELECT COUNT(*) as total FROM properties';
    if (type && type !== 'all') {
      countSql += ' WHERE type = ?';
      if (location) {
        countSql += ' AND location LIKE ?';
      }
    } else if (location) {
      countSql += ' WHERE location LIKE ?';
    }

    db.query(countSql, params.slice(0, params.length - 2), (countErr, countResult) => {
      if (countErr) {
        console.error('Error counting properties:', countErr);
        return res.status(500).send('Database count error');
      }
      const totalProperties = countResult[0].total;
      const totalPages = Math.ceil(totalProperties / limit);

      res.send({
        properties: result,
        totalPages: totalPages,
        currentPage: parseInt(page)
      });
    });
  });
});



app.get('/properties/:id', (req, res) => {
  const language = req.query.lang || 'en';
  const titleColumn = `title_${language}` in req.query ? `title_${language}` : `title_ar`;
  const descriptionColumn = `description_${language}` in req.query ? `description_${language}` : `description_ar`;

  const sql = `
    SELECT p.property_id, p.${titleColumn} as title, p.price, p.location, p.bedrooms, p.bathrooms, p.salon, p.area, p.type, p.available, p.floors, p.availability_date, pi.image_url, p.${descriptionColumn} as description
    FROM properties p 
    LEFT JOIN property_images pi ON p.property_id = pi.property_id 
    WHERE p.property_id = ?
  `;
  db.query(sql, [req.params.id], (err, result) => {
    if (err) {
      console.error('Error querying property:', err);
      return res.status(500).send('Database query error');
    }
    res.send(result);
  });
});

app.post('/properties', upload.array('images', 20), async (req, res) => {
  try {
    const newProperty = {
      title_ar: req.body.title_ar,      
      description_ar: req.body.description_ar,
      price: req.body.price,
      location: req.body.location,
      bedrooms: req.body.bedrooms,
      salon: req.body.salon,
      bathrooms: req.body.bathrooms,
      area: req.body.area,
      type: req.body.type,
      available: req.body.type === 'rent' ? true : req.body.available,
      floors: req.body.floors, // Add floors field
      availability_date: req.body.availability_date // Add availability date field
    };

    console.log('New property data:', newProperty);

    const sql = 'INSERT INTO properties SET ?';
    db.query(sql, newProperty, (err, result) => {
      if (err) {
        console.error('Error inserting property:', err);
        return res.status(500).send('Database insertion error');
      }
      const propertyId = result.insertId;
      const images = req.files.map(file => [propertyId, file.filename]);

      const imageSql = 'INSERT INTO property_images (property_id, image_url) VALUES ?';
      db.query(imageSql, [images], (err, result) => {
        if (err) {
          console.error('Error inserting property images:', err);
          return res.status(500).send('Database insertion error');
        }
        res.send(result);
      });
    });
  } catch (error) {
    console.error('Error processing request:', error);
    res.status(500).send('Error processing request');
  }
});

app.put('/properties/:id', upload.array('images', 20), async (req, res) => {
  try {
    const updatedProperty = {
      title_ar: req.body.title_ar,
      description_ar: req.body.description_ar,
      price: req.body.price,
      location: req.body.location,
      bedrooms: req.body.bedrooms,
      salon: req.body.salon,
      bathrooms: req.body.bathrooms,
      area: req.body.area,
      type: req.body.type,
      available: req.body.available,
      floors: req.body.floors, // Add floors field
      availability_date: req.body.availability_date // Add availability date field
    };

    console.log('Updated property data:', updatedProperty);

    const sql = 'UPDATE properties SET ? WHERE property_id = ?';
    db.query(sql, [updatedProperty, req.params.id], (err, result) => {
      if (err) {
        console.error('Error updating property:', err);
        return res.status(500).send('Database update error');
      }

      if (req.files && req.files.length) {
        const selectImagesSql = 'SELECT image_url FROM property_images WHERE property_id = ?';
        db.query(selectImagesSql, [req.params.id], (err, images) => {
          if (err) {
            console.error('Error querying property images:', err);
            return res.status(500).send('Database query error');
          }

          images.forEach(image => {
            const filePath = path.join(__dirname, 'uploads', image.image_url);
            fs.unlink(filePath, (err) => {
              if (err) console.error(`Error deleting file: ${filePath}`, err);
            });
          });

          const deleteImagesSql = 'DELETE FROM property_images WHERE property_id = ?';
          db.query(deleteImagesSql, [req.params.id], (err, deleteResult) => {
            if (err) {
              console.error('Error deleting property images:', err);
              return res.status(500).send('Database deletion error');
            }

            const newImages = req.files.map(file => [req.params.id, file.filename]);
            const imageSql = 'INSERT INTO property_images (property_id, image_url) VALUES ?';
            db.query(imageSql, [newImages], (err, insertResult) => {
              if (err) {
                console.error('Error inserting property images:', err);
                return res.status(500).send('Database insertion error');
              }
              res.send(insertResult);
            });
          });
        });
      } else {
        res.send(result);
      }
    });
  } catch (error) {
    console.error('Error processing request:', error);
    res.status(500).send('Error processing request');
  }
});

app.put('/properties/:id/availability', (req, res) => {
  const { available, availability_date } = req.body;
  const sql = 'UPDATE properties SET available = ?, availability_date = ? WHERE property_id = ?';
  db.query(sql, [available, available ? null : availability_date, req.params.id], (err, result) => {
    if (err) {
      console.error('Error updating availability:', err);
      return res.status(500).send('Database update error');
    }
    res.send(result);
  });
});

app.delete('/properties/:id', (req, res) => {
  const propertyId = req.params.id;

  const selectImagesSql = 'SELECT image_url FROM property_images WHERE property_id = ?';
  db.query(selectImagesSql, [propertyId], (err, images) => {
    if (err) {
      console.error('Error querying property images:', err);
      return res.status(500).send('Database query error');
    }

    images.forEach(image => {
      const filePath = path.join(__dirname, 'uploads', image.image_url);
      fs.unlink(filePath, (err) => {
        if (err) console.error(`Error deleting file: ${filePath}`, err);
      });
    });

    const deleteImagesSql = 'DELETE FROM property_images WHERE property_id = ?';
    db.query(deleteImagesSql, [propertyId], (err, result) => {
      if (err) {
        console.error('Error deleting property images:', err);
        return res.status(500).send('Database deletion error');
      }

      const deletePropertySql = 'DELETE FROM properties WHERE property_id = ?';
      db.query(deletePropertySql, [propertyId], (err, result) => {
        if (err) {
          console.error('Error deleting property:', err);
          return res.status(500).send('Database deletion error');
        }
        res.send(result);
      });
    });
  });
});

app.post('/login', (req, res) => {
  const { email, password } = req.body;

  db.query('SELECT * FROM users WHERE email = ?', [email], async (err, result) => {
    if (err) {
      console.error('Database error:', err);
      return res.status(500).json({ success: false, message: 'Internal server error' });
    }
    if (result.length === 0) {
      console.log('Invalid email:', email);
      return res.status(401).json({ success: false, message: 'Invalid email or password' });
    }

    const user = result[0];
    const isPasswordValid = await bcrypt.compare(password, user.password);
    if (!isPasswordValid) {
      console.log('Invalid password for email:', email);
      return res.status(401).json({ success: false, message: 'Invalid email or password' });
    }

    const token = jwt.sign({ id: user.id }, 'your_jwt_secret', { expiresIn: '1h' });
    res.json({ success: true, token, email: user.email });
  });
});

// app.get('/news', (req, res) => {
//   const lang = req.query.lang || 'en';
//   const titleColumn = `title_${lang}`;
//   const contentColumn = `content_${lang}`;

//   const sql = `
//     SELECT id, ${titleColumn} as title, ${contentColumn} as content, image_url, published_at
//     FROM news
//   `;

//   db.query(sql, (err, result) => {
//     if (err) {
//       console.error('Error querying news:', err);
//       return res.status(500).send('Database query error');
//     }
//     res.send(result);
//   });
// });
app.get('/news', (req, res) => {
  const lang = req.query.lang || 'ar';
  const titleColumn = `title_${lang}`;
  const contentColumn = `content_${lang}`;

  const sql = `
    SELECT id, ${titleColumn} as title, ${contentColumn} as content, image_url, published_at
    FROM news
  `;

  db.query(sql, (err, result) => {
    if (err) {
      console.error('Error querying news:', err);
      return res.status(500).send('Database query error');
    }
    res.send(result);
  });
});

// app.post('/news', upload.single('image'), async (req, res) => {
//   try {
//     const newNews = {
//       title_ar: req.body.title,
//       content_ar: req.body.content,
//       image_url: req.file ? req.file.filename : ''
//     };

//     console.log('New news data:', newNews);

//     const sql = 'INSERT INTO news SET ?';
//     db.query(sql, newNews, (err, result) => {
//       if (err) {
//         console.error('Error inserting news:', err);
//         return res.status(500).send('Database insertion error');
//       }
//       res.send(result);
//     });
//   } catch (error) {
//     console.error('Error processing request:', error);
//     res.status(500).send('Error processing request');
//   }
// });
app.post('/news', upload.single('image'), async (req, res) => {
  try {
    const newNews = {
      title_ar: req.body.title_ar,
      content_ar: req.body.content_ar,
      image_url: req.file ? req.file.filename : ''
    };

    console.log('New news data:', newNews);

    const sql = 'INSERT INTO news SET ?';
    db.query(sql, newNews, (err, result) => {
      if (err) {
        console.error('Error inserting news:', err);
        return res.status(500).send('Database insertion error');
      }
      res.send(result);
    });
  } catch (error) {
    console.error('Error processing request:', error);
    res.status(500).send('Error processing request');
  }
});


// app.delete('/news/:id', (req, res) => {
//   const newsId = req.params.id;

//   const selectImageSql = 'SELECT image_url FROM news WHERE id = ?';
//   db.query(selectImageSql, [newsId], (err, result) => {
//     if (err) {
//       console.error('Error querying news image:', err);
//       return res.status(500).send('Database query error');
//     }

//     const imageUrl = result[0].image_url;
//     const filePath = path.join(__dirname, 'uploads', imageUrl);

//     fs.unlink(filePath, (err) => {
//       if (err) console.error(`Error deleting file: ${filePath}`, err);

//       const deleteNewsSql = 'DELETE FROM news WHERE id = ?';
//       db.query(deleteNewsSql, [newsId], (err, result) => {
//         if (err) {
//           console.error('Error deleting news:', err);
//           return res.status(500).send('Database deletion error');
//         }
//         res.send(result);
//       });
//     });
//   });
// });
app.delete('/news/:id', (req, res) => {
  const newsId = req.params.id;

  const selectImageSql = 'SELECT image_url FROM news WHERE id = ?';
  db.query(selectImageSql, [newsId], (err, result) => {
    if (err) {
      console.error('Error querying news image:', err);
      return res.status(500).send('Database query error');
    }

    const imageUrl = result[0]?.image_url;
    if (imageUrl) {
      const filePath = path.join(__dirname, 'uploads', imageUrl);

      fs.unlink(filePath, (err) => {
        if (err) console.error(`Error deleting file: ${filePath}`, err);

        const deleteNewsSql = 'DELETE FROM news WHERE id = ?';
        db.query(deleteNewsSql, [newsId], (err, result) => {
          if (err) {
            console.error('Error deleting news:', err);
            return res.status(500).send('Database deletion error');
          }
          res.send(result);
        });
      });
    } else {
      const deleteNewsSql = 'DELETE FROM news WHERE id = ?';
      db.query(deleteNewsSql, [newsId], (err, result) => {
        if (err) {
          console.error('Error deleting news:', err);
          return res.status(500).send('Database deletion error');
        }
        res.send(result);
      });
    }
  });
});

app.post('/contact-submissions', (req, res) => {
  const newSubmission = {
    name: req.body.name,
    email: req.body.email,
    phone: req.body.phone, 
    subject: req.body.subject,
    message: req.body.message
  };

  const sql = 'INSERT INTO contact_submissions SET ?';
  db.query(sql, newSubmission, (err, result) => {
    if (err) {
      console.error('Error inserting contact submission:', err);
      return res.status(500).send('Database insertion error');
    }
    res.send(result);
  });
});

app.get('/contact-submissions', (req, res) => {
  const sql = 'SELECT * FROM contact_submissions ORDER BY created_at DESC';
  db.query(sql, (err, result) => {
    if (err) {
      console.error('Error querying contact submissions:', err);
      return res.status(500).send('Database query error');
    }
    res.send(result);
  });
});

app.delete('/contact-submissions/:id', (req, res) => {
  const sql = 'DELETE FROM contact_submissions WHERE id = ?';
  db.query(sql, [req.params.id], (err, result) => {
    if (err) {
      console.error('Error deleting contact submission:', err);
      return res.status(500).send('Database deletion error');
    }
    res.send(result);
  });
});

const PORT = process.env.PORT || 5000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));


// ---------------------------code for production--------------------------------------

// const express = require('express');
// const bodyParser = require('body-parser');
// const mysql = require('mysql');
// const cors = require('cors');
// const bcrypt = require('bcryptjs');
// const jwt = require('jsonwebtoken');
// const multer = require('multer');
// const path = require('path');
// const fs = require('fs');

// const app = express();
// const router = express.Router();

// app.use(bodyParser.json());
// app.use(cors());

// const db = mysql.createConnection({
//   host: 'localhost',
//   user: 'alaqgxtb_admin',
//   password: '}R8rs!T3H[@K',
//   database: 'alaqgxtb_alaqariyya'
// });


// db.connect(err => {
//   if (err) throw err;
//   console.log('MySQL Connected...');
// });

// const storage = multer.diskStorage({
//   destination: (req, file, cb) => {
//     const uploadPath = path.join(__dirname, 'uploads');
//     if (!fs.existsSync(uploadPath)) {
//       fs.mkdirSync(uploadPath);
//     }
//     cb(null, 'uploads');
//   },
//   filename: (req, file, cb) => {
//     cb(null, Date.now() + path.extname(file.originalname));
//   }
// });

// const upload = multer({ storage });

// router.use('/uploads', express.static(path.join(__dirname, 'uploads')));

// router.get('/', (req, res) => {
//   res.send('Welcome to the Node.js backend for ALAQARIYYA');
// });


// router.get('/properties', (req, res) => {
//   const { type, location, page = 1, limit = 8, lang = 'en' } = req.query;
//   const offset = (page - 1) * limit;
//   const titleColumn = `title_${lang}` in req.query ? `title_${lang}` : `title_ar`;
//   const descriptionColumn = `description_${lang}` in req.query ? `description_${lang}` : `description_ar`;

//   let sql = `
//     SELECT p.property_id, p.${titleColumn} as title, p.price, p.location, p.bedrooms, p.bathrooms, p.salon, p.area, p.type, p.available, p.floors, p.availability_date, pi.image_url, p.${descriptionColumn} as description
//     FROM properties p
//     LEFT JOIN (
//       SELECT property_id, MIN(image_url) as image_url
//       FROM property_images
//       GROUP BY property_id
//     ) pi ON p.property_id = pi.property_id
//   `;
//   const params = [];

//   if (type && type !== 'all') {
//     sql += ' WHERE p.type = ?';
//     params.push(type);
//   }

//   if (location) {
//     sql += params.length ? ' AND p.location LIKE ?' : ' WHERE p.location LIKE ?';
//     params.push(`%${location}%`);
//   }

//   sql += ' LIMIT ? OFFSET ?';
//   params.push(parseInt(limit), parseInt(offset));

//   db.query(sql, params, (err, result) => {
//     if (err) {
//       console.error('Error querying properties:', err);
//       return res.status(500).send('Database query error');
//     }

//     let countSql = 'SELECT COUNT(*) as total FROM properties';
//     if (type && type !== 'all') {
//       countSql += ' WHERE type = ?';
//       if (location) {
//         countSql += ' AND location LIKE ?';
//       }
//     } else if (location) {
//       countSql += ' WHERE location LIKE ?';
//     }

//     db.query(countSql, params.slice(0, params.length - 2), (countErr, countResult) => {
//       if (countErr) {
//         console.error('Error counting properties:', countErr);
//         return res.status(500).send('Database count error');
//       }
//       const totalProperties = countResult[0].total;
//       const totalPages = Math.ceil(totalProperties / limit);

//       res.send({
//         properties: result,
//         totalPages: totalPages,
//         currentPage: parseInt(page)
//       });
//     });
//   });
// });



// router.get('/properties/:id', (req, res) => {
//   const language = req.query.lang || 'en';
//   const titleColumn = `title_${language}` in req.query ? `title_${language}` : `title_ar`;
//   const descriptionColumn = `description_${language}` in req.query ? `description_${language}` : `description_ar`;

//   const sql = `
//     SELECT p.property_id, p.${titleColumn} as title, p.price, p.location, p.bedrooms, p.bathrooms, p.salon, p.area, p.type, p.available, p.floors, p.availability_date, pi.image_url, p.${descriptionColumn} as description
//     FROM properties p 
//     LEFT JOIN property_images pi ON p.property_id = pi.property_id 
//     WHERE p.property_id = ?
//   `;
//   db.query(sql, [req.params.id], (err, result) => {
//     if (err) {
//       console.error('Error querying property:', err);
//       return res.status(500).send('Database query error');
//     }
//     res.send(result);
//   });
// });

// router.post('/properties', upload.array('images', 20), async (req, res) => {
//   try {
//     const newProperty = {
//       title_ar: req.body.title_ar,      
//       description_ar: req.body.description_ar,
//       price: req.body.price,
//       location: req.body.location,
//       bedrooms: req.body.bedrooms,
//       salon: req.body.salon,
//       bathrooms: req.body.bathrooms,
//       area: req.body.area,
//       type: req.body.type,
//       available: req.body.type === 'rent' ? true : req.body.available,
//       floors: req.body.floors, // Add floors field
//       availability_date: req.body.availability_date // Add availability date field
//     };

//     console.log('New property data:', newProperty);

//     const sql = 'INSERT INTO properties SET ?';
//     db.query(sql, newProperty, (err, result) => {
//       if (err) {
//         console.error('Error inserting property:', err);
//         return res.status(500).send('Database insertion error');
//       }
//       const propertyId = result.insertId;
//       const images = req.files.map(file => [propertyId, file.filename]);

//       const imageSql = 'INSERT INTO property_images (property_id, image_url) VALUES ?';
//       db.query(imageSql, [images], (err, result) => {
//         if (err) {
//           console.error('Error inserting property images:', err);
//           return res.status(500).send('Database insertion error');
//         }
//         res.send(result);
//       });
//     });
//   } catch (error) {
//     console.error('Error processing request:', error);
//     res.status(500).send('Error processing request');
//   }
// });

// router.put('/properties/:id', upload.array('images', 20), async (req, res) => {
//   try {
//     const updatedProperty = {
//       title_ar: req.body.title_ar,
//       description_ar: req.body.description_ar,
//       price: req.body.price,
//       location: req.body.location,
//       bedrooms: req.body.bedrooms,
//       salon: req.body.salon,
//       bathrooms: req.body.bathrooms,
//       area: req.body.area,
//       type: req.body.type,
//       available: req.body.available,
//       floors: req.body.floors, // Add floors field
//       availability_date: req.body.availability_date // Add availability date field
//     };

//     console.log('Updated property data:', updatedProperty);

//     const sql = 'UPDATE properties SET ? WHERE property_id = ?';
//     db.query(sql, [updatedProperty, req.params.id], (err, result) => {
//       if (err) {
//         console.error('Error updating property:', err);
//         return res.status(500).send('Database update error');
//       }

//       if (req.files && req.files.length) {
//         const selectImagesSql = 'SELECT image_url FROM property_images WHERE property_id = ?';
//         db.query(selectImagesSql, [req.params.id], (err, images) => {
//           if (err) {
//             console.error('Error querying property images:', err);
//             return res.status(500).send('Database query error');
//           }

//           images.forEach(image => {
//             const filePath = path.join(__dirname, 'uploads', image.image_url);
//             fs.unlink(filePath, (err) => {
//               if (err) console.error(`Error deleting file: ${filePath}`, err);
//             });
//           });

//           const deleteImagesSql = 'DELETE FROM property_images WHERE property_id = ?';
//           db.query(deleteImagesSql, [req.params.id], (err, deleteResult) => {
//             if (err) {
//               console.error('Error deleting property images:', err);
//               return res.status(500).send('Database deletion error');
//             }

//             const newImages = req.files.map(file => [req.params.id, file.filename]);
//             const imageSql = 'INSERT INTO property_images (property_id, image_url) VALUES ?';
//             db.query(imageSql, [newImages], (err, insertResult) => {
//               if (err) {
//                 console.error('Error inserting property images:', err);
//                 return res.status(500).send('Database insertion error');
//               }
//               res.send(insertResult);
//             });
//           });
//         });
//       } else {
//         res.send(result);
//       }
//     });
//   } catch (error) {
//     console.error('Error processing request:', error);
//     res.status(500).send('Error processing request');
//   }
// });

// router.put('/properties/:id/availability', (req, res) => {
//   const { available, availability_date } = req.body;
//   const sql = 'UPDATE properties SET available = ?, availability_date = ? WHERE property_id = ?';
//   db.query(sql, [available, available ? null : availability_date, req.params.id], (err, result) => {
//     if (err) {
//       console.error('Error updating availability:', err);
//       return res.status(500).send('Database update error');
//     }
//     res.send(result);
//   });
// });

// router.delete('/properties/:id', (req, res) => {
//   const propertyId = req.params.id;

//   const selectImagesSql = 'SELECT image_url FROM property_images WHERE property_id = ?';
//   db.query(selectImagesSql, [propertyId], (err, images) => {
//     if (err) {
//       console.error('Error querying property images:', err);
//       return res.status(500).send('Database query error');
//     }

//     images.forEach(image => {
//       const filePath = path.join(__dirname, 'uploads', image.image_url);
//       fs.unlink(filePath, (err) => {
//         if (err) console.error(`Error deleting file: ${filePath}`, err);
//       });
//     });

//     const deleteImagesSql = 'DELETE FROM property_images WHERE property_id = ?';
//     db.query(deleteImagesSql, [propertyId], (err, result) => {
//       if (err) {
//         console.error('Error deleting property images:', err);
//         return res.status(500).send('Database deletion error');
//       }

//       const deletePropertySql = 'DELETE FROM properties WHERE property_id = ?';
//       db.query(deletePropertySql, [propertyId], (err, result) => {
//         if (err) {
//           console.error('Error deleting property:', err);
//           return res.status(500).send('Database deletion error');
//         }
//         res.send(result);
//       });
//     });
//   });
// });

// router.post('/login', (req, res) => {
//   const { email, password } = req.body;

//   db.query('SELECT * FROM users WHERE email = ?', [email], async (err, result) => {
//     if (err) {
//       console.error('Database error:', err);
//       return res.status(500).json({ success: false, message: 'Internal server error' });
//     }
//     if (result.length === 0) {
//       console.log('Invalid email:', email);
//       return res.status(401).json({ success: false, message: 'Invalid email or password' });
//     }

//     const user = result[0];
//     const isPasswordValid = await bcrypt.compare(password, user.password);
//     if (!isPasswordValid) {
//       console.log('Invalid password for email:', email);
//       return res.status(401).json({ success: false, message: 'Invalid email or password' });
//     }

//     const token = jwt.sign({ id: user.id }, 'your_jwt_secret', { expiresIn: '1h' });
//     res.json({ success: true, token, email: user.email });
//   });
// });

// router.get('/news', (req, res) => {
//   const lang = req.query.lang || 'en';
//   const titleColumn = `title_${lang}`;
//   const contentColumn = `content_${lang}`;

//   const sql = `
//     SELECT id, ${titleColumn} as title, ${contentColumn} as content, image_url, published_at
//     FROM news
//   `;

//   db.query(sql, (err, result) => {
//     if (err) {
//       console.error('Error querying news:', err);
//       return res.status(500).send('Database query error');
//     }
//     res.send(result);
//   });
// });

// router.post('/news', upload.single('image'), async (req, res) => {
//   try {
//     const newNews = {
//       title_ar: req.body.title,
//       content_ar: req.body.content,
//       image_url: req.file ? req.file.filename : ''
//     };

//     console.log('New news data:', newNews);

//     const sql = 'INSERT INTO news SET ?';
//     db.query(sql, newNews, (err, result) => {
//       if (err) {
//         console.error('Error inserting news:', err);
//         return res.status(500).send('Database insertion error');
//       }
//       res.send(result);
//     });
//   } catch (error) {
//     console.error('Error processing request:', error);
//     res.status(500).send('Error processing request');
//   }
// });

// router.delete('/news/:id', (req, res) => {
//   const newsId = req.params.id;

//   const selectImageSql = 'SELECT image_url FROM news WHERE id = ?';
//   db.query(selectImageSql, [newsId], (err, result) => {
//     if (err) {
//       console.error('Error querying news image:', err);
//       return res.status(500).send('Database query error');
//     }

//     const imageUrl = result[0].image_url;
//     const filePath = path.join(__dirname, 'uploads', imageUrl);

//     fs.unlink(filePath, (err) => {
//       if (err) console.error(`Error deleting file: ${filePath}`, err);

//       const deleteNewsSql = 'DELETE FROM news WHERE id = ?';
//       db.query(deleteNewsSql, [newsId], (err, result) => {
//         if (err) {
//           console.error('Error deleting news:', err);
//           return res.status(500).send('Database deletion error');
//         }
//         res.send(result);
//       });
//     });
//   });
// });

// router.post('/contact-submissions', (req, res) => {
//   const newSubmission = {
//     name: req.body.name,
//     email: req.body.email,
//     phone: req.body.phone, 
//     subject: req.body.subject,
//     message: req.body.message
//   };

//   const sql = 'INSERT INTO contact_submissions SET ?';
//   db.query(sql, newSubmission, (err, result) => {
//     if (err) {
//       console.error('Error inserting contact submission:', err);
//       return res.status(500).send('Database insertion error');
//     }
//     res.send(result);
//   });
// });

// router.get('/contact-submissions', (req, res) => {
//   const sql = 'SELECT * FROM contact_submissions ORDER BY created_at DESC';
//   db.query(sql, (err, result) => {
//     if (err) {
//       console.error('Error querying contact submissions:', err);
//       return res.status(500).send('Database query error');
//     }
//     res.send(result);
//   });
// });

// router.delete('/contact-submissions/:id', (req, res) => {
//   const sql = 'DELETE FROM contact_submissions WHERE id = ?';
//   db.query(sql, [req.params.id], (err, result) => {
//     if (err) {
//       console.error('Error deleting contact submission:', err);
//       return res.status(500).send('Database deletion error');
//     }
//     res.send(result);
//   });
// });

// app.use('/nodeapp', router);


// const PORT = process.env.PORT || 5000;
// app.listen(PORT, () => console.log(`Server running on port ${PORT}`));