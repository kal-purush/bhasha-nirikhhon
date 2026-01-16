const express = require('express');
const mongoose = require('mongoose');
const cors = require('cors');
const nodemailer = require("nodemailer");
const bodyParser = require('body-parser');
const multer = require('multer');
const bcrypt = require('bcryptjs');
const jwt = require('jsonwebtoken');
const path = require('path');
const fs = require('fs');
const xlsx = require('xlsx');
const xl = require('excel4node');
require('dotenv').config();
const http = require("http");
const { Server } = require("socket.io");

const app = express();
const server = http.createServer(app);

const io = new Server(server, {
    cors: {
        origin: "http://localhost:5173",
        methods: ["GET", "POST"]
    }
});

app.use(cors());
app.use(bodyParser.json());
app.use('/uploads', express.static('uploads')); // Serve images statically

// 🔹 **MongoDB Connection**
mongoose.connect('mongodb://localhost:27017/maintenanceDB', {
    useNewUrlParser: true,
    useUnifiedTopology: true
}).then(() => console.log("✅ MongoDB Connected"))
  .catch(err => console.error(err));


// 🔹 **Define Schemas**
const IssueSchema = new mongoose.Schema({
    TPM: String,
    Name: String,
    category: String,
    NOP: String,
    Line: String,
    status: { type: String, default: "Pending" },
    description: String,
    images: [String],
    isRedTagClosed: String, // "Yes" or "No"
    whoClosedRedTag: String, // Person who closed it
    howRedTagClosed: String, // How it was closed
    createdAt: { type: Date, default: Date.now }
});
const KaizenSchema = new mongoose.Schema({
    beforeImage: String,
    afterImage: String,
    description: String,
    createdAt: { type: Date, default: Date.now }
});
const Kaizen = mongoose.model("Kaizen", KaizenSchema);
const UserSchema = new mongoose.Schema({
    name: String,
    email: { type: String, unique: true },
    password: String,
    role: { type: String, default: "user" }  // ✅ New: Role field (admin/user)
});

const TPMThemeSchema = new mongoose.Schema({
    day: { type: String, unique: true, required: true }, // 🔹 **e.g., Monday, Tuesday**
    theme: { type: String, required: true }
});

// 🔹 **Create Models**
const Issue = mongoose.model('Issue', IssueSchema);
const User = mongoose.model('User', UserSchema);
const TPMTheme = mongoose.model("TPMTheme", TPMThemeSchema);

//CHatbox
const ChatSchema = new mongoose.Schema({
    username: String,
    message: String,
    timestamp: { type: Date, default: Date.now }
});

const Chat = mongoose.model('Chat', ChatSchema);
const storage = multer.diskStorage({
    destination: (req, file, cb) => {
        cb(null, "uploads/");
    },
    filename: (req, file, cb) => {
        cb(null, Date.now() + path.extname(file.originalname));
    }
});
const upload = multer({ storage: storage });
// Real-time Chat with Socket.io
io.on("connection", (socket) => {
    console.log("New user connected:", socket.id);

    // Send previous messages from database
    Chat.find().sort({ timestamp: 1 }).then(messages => {
        socket.emit("previousMessages", messages);
    });

    // Listen for new messages
    socket.on("sendMessage", async (data) => {
        const newMessage = new Chat({
            username: data.username,
            message: data.message
        });
        await newMessage.save();

        io.emit("receiveMessage", newMessage); // Broadcast to all users
    });

    // Handle disconnection
    socket.on("disconnect", () => {
        console.log("User disconnected:", socket.id);
    });
});
// 🔹 **JWT Middleware**
const verifyToken = (req, res, next) => {
    try {
        const authHeader = req.header("Authorization");

        // Ensure token exists
        if (!authHeader) {
            return res.status(401).json({ message: "⛔ Access Denied. No token provided." });
        }

        // Ensure Bearer format is used
        const token = authHeader.startsWith("Bearer ") ? authHeader.split(" ")[1] : authHeader;

        // Verify token using environment secret
        const verified = jwt.verify(token, process.env.JWT_SECRET || "yourSecretKey");

        // Attach user data (including role) to request
        req.user = verified;

        // Proceed to next middleware
        next();
    } catch (error) {
        // Handle specific JWT errors for better debugging
        if (error.name === "TokenExpiredError") {
            return res.status(401).json({ message: "🔓 Session expired. Please log in again." });
        } else if (error.name === "JsonWebTokenError") {
            return res.status(403).json({ message: "❌ Invalid Token" });
        } else {
            return res.status(500).json({ message: "⚠️ Authentication error. Try again." });
        }
    }
};



// Admin Check Middleware
const isAdmin = (req, res, next) => {
    if (!req.user || req.user.role !== "admin") {
        return res.status(403).json({ message: "Access Denied. Admins only." });
    }
    next();
};
app.post('/issues', upload.array('images', 10), async (req, res) => {
    try {
        console.log("Received Data:", req.body);
        console.log("Uploaded Files:", req.files);

        const imagePaths = req.files.map(file => `/uploads/${file.filename}`);
        const newIssue = new Issue({ ...req.body, images: imagePaths });

        await newIssue.save();
        res.status(201).json({ message: "Issue created successfully!", newIssue });
    } catch (error) {
        console.error("Error creating issue:", error);
        res.status(500).json({ message: "Server error while creating issue" });
    }
});


// 🔹 **Update TPM Theme (Only Admins)**
app.post('/update-theme', async (req, res) => {
    try {
        const { day, theme } = req.body;

        if (!day || !theme) {
            return res.status(400).json({ message: "Both day and theme are required!" });
        }

        const updatedTheme = await TPMTheme.findOneAndUpdate(
            { day },
            { theme },
            { upsert: true, new: true }
        );

        res.json({ message: `✅ Theme for ${day} updated successfully!`, updatedTheme });
    } catch (error) {
        res.status(500).json({ message: error.message });
    }
});
app.get('/protected', verifyToken, async (req, res) => {
    try {
        const user = await User.findById(req.user.id).select("-password");
        if (!user) {
            return res.status(404).json({ message: "User not found" });
        }
        res.json({ user });
    } catch (error) {
        res.status(500).json({ message: "Server error" });
    }
});


// 🔹 **Get All TPM Themes**
app.get('/tpm-themes', async (req, res) => {
    try {
        const themes = await TPMTheme.find();
        res.json(themes);
    } catch (error) {
        res.status(500).json({ message: error.message });
    }
});

// 🔹 **Register User**
app.post('/register', async (req, res) => {
    try {
        const { name, email, password } = req.body;
        const existingUser = await User.findOne({ email });
        if (existingUser) return res.status(400).json({ message: "User already exists!" });

        const hashedPassword = await bcrypt.hash(password, 10);
        const newUser = new User({ name, email, password: hashedPassword });
        await newUser.save();

        res.status(201).json({ message: "✅ User registered successfully!" });
    } catch (error) {
        res.status(500).json({ message: error.message });
    }
});

// 🔹 **Login User**
app.post('/login', async (req, res) => {
    try {
        const { email, password } = req.body;
        const user = await User.findOne({ email });
        if (!user) return res.status(400).json({ message: "Invalid Email or Password" });

        const isMatch = await bcrypt.compare(password, user.password);
        if (!isMatch) return res.status(400).json({ message: "Invalid Email or Password" });

        const token = jwt.sign({ id: user._id }, "yourSecretKey", { expiresIn: "1h" });

        res.json({ token, user: { name: user.name, email: user.email, role: user.role } });
    } catch (error) {
        res.status(500).json({ message: error.message });
    }
});

// 🔹 **Get All Issues**
app.get('/issues', async (req, res) => {
    try {
        const issues = await Issue.find();
        res.status(200).json(issues);
    } catch (error) {
        res.status(500).json({ message: error.message });
    }
});

// 🔹 **Delete Issue**
app.delete('/issues/:id', async (req, res) => {
    try {
        const issue = await Issue.findByIdAndDelete(req.params.id);
        if (!issue) return res.status(404).json({ message: "Issue not found" });

        res.status(200).json({ message: "✅ Issue deleted successfully" });
    } catch (error) {
        res.status(500).json({ message: "Server error while deleting issue" });
    }
});

// 🔹 **Export Issues to Excel**
app.get('/export-excel', async (req, res) => {
    try {
        const issues = await Issue.find();

        if (issues.length === 0) {
            return res.status(404).json({ message: "No issues found to export." });
        }

        // Create a new Excel workbook and worksheet
        const wb = new xl.Workbook();
        const ws = wb.addWorksheet('Issues');

        // Define column headers
        const headers = ["TPM", "Name", "Category", "NOP", "Line", "Status", "Description", "Created At", "Images"];
        headers.forEach((header, i) => ws.cell(1, i + 1).string(header).style({ bold: true }));

        // Populate rows with issue data
        issues.forEach((issue, rowIndex) => {
            if (!issue) {
                console.error(`Skipping undefined issue at index ${rowIndex}`);
                return;
            }

            const row = rowIndex + 2; // Start from row 2 (row 1 is headers)
            ws.cell(row, 1).string(issue.TPM || "");
            ws.cell(row, 2).string(issue.Name || "");
            ws.cell(row, 3).string(issue.category || "");
            ws.cell(row, 4).string(issue.NOP || "");
            ws.cell(row, 5).string(issue.Line || "");
            ws.cell(row, 6).string(issue.status || "");
            ws.cell(row, 7).string(issue.description || "");
            ws.cell(row, 8).string(issue.createdAt.toISOString() || "");

            // Insert Images into Excel
            let col = 9;
            if (issue.images && issue.images.length > 0) {
                issue.images.forEach((imgPath, imgIndex) => {
                    const fullPath = path.join(__dirname, imgPath);
                    if (fs.existsSync(fullPath)) {
                        ws.addImage({
                            path: fullPath,
                            type: 'picture',
                            position: {
                                type: 'twoCellAnchor',
                                from: { col: col, row: row },
                                to: { col: col + 3, row: row + 3 },
                            }
                        });
                    }
                    col += 4;
                });
            }
        });

        // Save and send file
        const filePath = './issues_report.xlsx';
        wb.write(filePath, () => {
            res.download(filePath, "issues_report.xlsx", (err) => {
                if (err) {
                    console.error("Download error:", err);
                    res.status(500).json({ message: "Error generating Excel file" });
                }
                fs.unlinkSync(filePath);
            });
        });

    } catch (error) {
        console.error("Error generating Excel file:", error);
        res.status(500).json({ message: error.message });
    }
});
app.post("/kaizen", upload.fields([{ name: "beforeImage" }, { name: "afterImage" }]), async (req, res) => {
    try {
        const beforeImagePath = req.files["beforeImage"][0].path;
        const afterImagePath = req.files["afterImage"][0].path;
        const { description } = req.body;

        const newKaizen = new Kaizen({
            beforeImage: `/${beforeImagePath}`,
            afterImage: `/${afterImagePath}`,
            description
        });

        await newKaizen.save();
        res.status(201).json({ message: "Kaizen post uploaded successfully!", newKaizen });
    } catch (error) {
        res.status(500).json({ message: "Server error while uploading Kaizen post", error });
    }
});

// 📌 API: Get all Kaizen Posts (for feed)
app.get("/kaizen", async (req, res) => {
    try {
        const kaizenPosts = await Kaizen.find().sort({ createdAt: -1 });
        res.status(200).json(kaizenPosts);
    } catch (error) {
        res.status(500).json({ message: "Server error while fetching Kaizen posts", error });
    }
});
app.delete("/kaizen/:id", async (req, res) => {
    try {
        const kaizen = await Kaizen.findById(req.params.id);
        if (!kaizen) return res.status(404).json({ message: "❌ Kaizen post not found" });

        // Delete images from the server
        if (kaizen.beforeImage) fs.unlinkSync("." + kaizen.beforeImage);
        if (kaizen.afterImage) fs.unlinkSync("." + kaizen.afterImage);

        await kaizen.deleteOne();
        res.json({ message: "✅ Kaizen post deleted successfully" });
    } catch (error) {
        console.error("❌ Error deleting Kaizen post:", error);
        res.status(500).json({ message: "Error deleting Kaizen post" });
    }
});



server.listen(5000, '0.0.0.0', () => console.log("🚀 Server running on port 5000"));