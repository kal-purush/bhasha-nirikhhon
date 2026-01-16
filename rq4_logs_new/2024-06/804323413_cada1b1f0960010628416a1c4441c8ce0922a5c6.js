const db = require('../config/db');

// Create a new company
exports.createCompany = (req, res) => {
  const { name, address, contact_number } = req.body;

  db.query(
    'INSERT INTO companies (name, address, contact_number) VALUES (?, ?, ?)',
    [name, address, contact_number],
    (err, result) => {
      if (err) {
        res.status(500).json({ message: 'Error occurred while creating company' });
      } else {
        res.status(201).json({ message: 'Company created successfully', companyId: result.insertId });
      }
    }
  );
};

// Add detail for a company
exports.addDetail = (req, res) => {
  const { company_id, detail_type, detail_value } = req.body;

  db.query(
    'INSERT INTO company_details (company_id, detail_type, detail_value) VALUES (?, ?, ?)',
    [company_id, detail_type, detail_value],
    (err, result) => {
      if (err) {
        res.status(500).json({ message: 'Error occurred while adding detail' });
      } else {
        res.status(201).json({ message: 'Detail added successfully' });
      }
    }
  );
};

// Get company details
exports.getCompanyDetails = (req, res) => {
  const { id } = req.params;

  db.query('SELECT * FROM companies WHERE id = ?', [id], (err, companyResult) => {
    if (err) {
      res.status(500).json({ message: 'Error occurred while fetching company details' });
    } else if (companyResult.length > 0) {
      db.query('SELECT * FROM company_details WHERE company_id = ?', [id], (err, detailResult) => {
        if (err) {
          res.status(500).json({ message: 'Error occurred while fetching company details' });
        } else {
          res.status(200).json({ company: companyResult[0], details: detailResult });
        }
      });
    } else {
      res.status(404).json({ message: 'Company not found' });
    }
  });
};

// Get all companies
exports.getAllCompanies = (req, res) => {
  db.query('SELECT * FROM companies', (err, result) => {
    if (err) {
      res.status(500).json({ message: 'Error occurred while fetching companies' });
    } else {
      res.status(200).json({ companies: result });
    }
  });
};

// Delete a company
exports.deleteCompany = (req, res) => {
  const { id } = req.params;

  db.query('DELETE FROM company_details WHERE company_id = ?', [id], (err, result) => {
    if (err) {
      res.status(500).json({ message: 'Error occurred while deleting associated details' });
    } else {
      db.query('DELETE FROM companies WHERE id = ?', [id], (err, result) => {
        if (err) {
          res.status(500).json({ message: 'Error occurred while deleting company' });
        } else {
          res.status(200).json({ message: 'Company deleted successfully' });
        }
      });
    }
  });
};

// Update a company
exports.updateCompany = (req, res) => {
  const { id } = req.params;
  const { name, address, contact_number } = req.body;

  db.query(
    'UPDATE companies SET name = ?, address = ?, contact_number = ? WHERE id = ?',
    [name, address, contact_number, id],
    (err, result) => {
      if (err) {
        res.status(500).json({ message: 'Error occurred while updating company' });
      } else {
        res.status(200).json({ message: 'Company updated successfully' });
      }
    }
  );
};