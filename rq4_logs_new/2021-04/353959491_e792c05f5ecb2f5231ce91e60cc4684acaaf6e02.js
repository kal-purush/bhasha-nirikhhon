const { ObjectId } = require("mongodb");
const connection = require("../models");

class BarangController {
  //get all datas
  async getAll(req, res) {
    try {
      // const transaksi = dbConnection.collection("transaksi"); // connect to table/collection transaksi
      const dbConnection = connection.db("penjualan"); // connect to db penjualan
      const barang = dbConnection.collection("barang"); // connect to db penjualan
      let data = await barang.find({}).toArray();

      //if no data
      if (data.length === 0) {
        return res.status(404).json({
          mesage: "barang ga ada",
        });
      }

      //if success
      return res.status(200).json({
        message: "success",
        data,
      });
    } catch (e) {
      //if failed
      return res.status(500).json({
        message: "internal server error",
        error: e,
      });
    }
  }
  // Get One Data
  async getOne(req, res) {
    try {
      const dbConnection = connection.db("penjualan"); // connect to db penjualan
      const barang = dbConnection.collection("barang"); // connect to db penjualan
      // Find one data
      let data = await barang.findOne({
        _id: new ObjectId(req.params.id),
      });

      // If success
      return res.status(200).json({
        message: "Success",
        data,
      });
    } catch (e) {
      // If failed
      return res.status(500).json({
        message: "Internal Server Error",
        error: e,
      });
    }
  }
  // Create data
  async create(req, res) {
    try {
      const dbConnection = connection.db("penjualan"); // connect to db penjualan
      const barang = dbConnection.collection("barang"); // connect to db penjualan
      // Insert data transaksi
      // console.log(req.body.barang)
      let data = await barang.insertOne({
        nama: req.body.nama,
        harga: req.body.harga,
        pemasok: req.body.pemasok
        
      });

      // If success
      return res.status(200).json({
        message: "Success",
        data: data.ops[0],
      });
    } catch (e) {
   
      return res.status(500).json({
        message: "Internal Server Error",
        error: e,
      });
    }
  }
  // Update data
  async update(req, res) {
    try {
      const dbConnection = connection.db("penjualan"); // connect to db penjualan
      const barang = dbConnection.collection("barang"); // connect to db penjualan
      // Update data transaksi
      await barang.updateOne(
        {
          _id: new ObjectId(req.params.id),
        },
        {
          $set: {
            nama: req.body.nama,
            harga: req.body.harga,
            pemasok: req.body.pemasok
          },
        }
      );

      // Find data that updated
      let data = await barang.findOne({
        _id: new ObjectId(req.params.id),
      });

      // If success
      return res.status(200).json({
        message: "Success",
        data,
      });
    } catch (e) {
      // If failed
      console.log(e)
      return res.status(500).json({
        message: "Internal Server Error",
        error: e,
      });
    }
  }

  // Delete Data
  async delete(req, res) {
    try {
      const dbConnection = connection.db("penjualan"); // connect to db penjualan
      const barang = dbConnection.collection("barang"); // connect to db penjualan
      // delete data depends on req.params.id
      let data = await barang.deleteOne({
        _id: new ObjectId(req.params.id),
      });

      // If success
      return res.status(200).json({
        message: "Success to delete barang",
      });
    } catch (e) {
      // If failed
      return res.status(500).json({
        message: "Internal Server Error",
        error: e,
      });
    }
  }
}

module.exports = new BarangController();