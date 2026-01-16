
const express = require("express");
const cors = require("cors");
// Import các lớp dịch vụ cho các bảng dữ liệu
const UserSRouter = require("./app/routes/user.route");
const ProductSRouter = require("./app/routes/hanghoa.route");
const EmployeeSRouter = require("./app/routes/nhanvien.route");
const OrderSRouter = require("./app/routes/dathang.route");
const DetailsRouter = require("./app/routes/chitietdathang.route");
const CartSRouter = require("./app/routes/giohang.route");


const XlsxPopulate = require('xlsx-populate');
 
// Load a new blank workbook
XlsxPopulate.fromBlankAsync()
    .then(workbook => {
        // Modify the workbook.
        workbook.sheet("Sheet1").cell("A1").value("This is neat!");
 
        // Write to file.
        return workbook.toFileAsync("./out.xlsx");
    });

const ApiError = require("./app/api-error");

const app = express();




app.use(cors());
app.use(express.json());

app.use("/api/users", UserSRouter);
app.use("/api/products", ProductSRouter);
app.use("/api/employees", EmployeeSRouter);
app.use("/api/orders", OrderSRouter);
app.use("/api/details", DetailsRouter);
app.use("/api/carts", CartSRouter);




// Xử lý lỗi 404 - Không Tìm thấy tài nguyên
app.use((req, res, next) => {
    return next(new ApiError(404, "Resource not found"));
});

// Xử lý lỗi chung 

app.use((error, req, res, next) => {
    return res.status(error.statusCode || 500).json({
        message: error.message || "Internal Server Error",

    });

});

app.get("/", (req, res) => {
    res.json({message: "Wellcome to blood donor resgistration application"});

});


module.exports = app;