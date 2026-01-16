import sqlite3

# Connect to SQLite database
conn = sqlite3.connect("freelancer_marketplace.db")
cursor = conn.cursor()

# Users table (Freelancers & Employers)
cursor.execute('''
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    password TEXT NOT NULL,
    role TEXT CHECK(role IN ('freelancer', 'employer')) NOT NULL,
    name TEXT NOT NULL,
    skills TEXT,
    experience TEXT,
    hourly_rate REAL,
    payment_method TEXT,
    company_name TEXT
)
''')

# Wallet table
cursor.execute('''
CREATE TABLE IF NOT EXISTS wallet (
    user_id INTEGER PRIMARY KEY,
    balance REAL DEFAULT 0.0,
    FOREIGN KEY (user_id) REFERENCES users(id)
)
''')

# Job Listings table
cursor.execute('''
CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    employer_id INTEGER NOT NULL,
    title TEXT NOT NULL,
    description TEXT NOT NULL,
    budget REAL NOT NULL,
    skills_required TEXT NOT NULL,
    duration TEXT NOT NULL,
    status TEXT CHECK(status IN ('open', 'in_progress', 'completed')) DEFAULT 'open',
    FOREIGN KEY (employer_id) REFERENCES users(id)
)
''')

# Job Applications table
cursor.execute('''
CREATE TABLE IF NOT EXISTS job_applications (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL,
    freelancer_id INTEGER NOT NULL,
    status TEXT CHECK(status IN ('applied', 'accepted', 'rejected', 'in_progress', 'completed')) DEFAULT 'applied',
    FOREIGN KEY (job_id) REFERENCES jobs(id),
    FOREIGN KEY (freelancer_id) REFERENCES users(id)
)
''')

# Milestones table
cursor.execute('''
CREATE TABLE IF NOT EXISTS milestones (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id INTEGER NOT NULL,
    freelancer_id INTEGER NOT NULL,
    title TEXT NOT NULL,
    status TEXT CHECK(status IN ('pending', 'approved', 'completed')) DEFAULT 'pending',
    payment REAL NOT NULL,
    FOREIGN KEY (job_id) REFERENCES jobs(id),
    FOREIGN KEY (freelancer_id) REFERENCES users(id)
)
''')

# Payments table
cursor.execute('''
CREATE TABLE IF NOT EXISTS payments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    employer_id INTEGER NOT NULL,
    freelancer_id INTEGER NOT NULL,
    job_id INTEGER NOT NULL,
    milestone_id INTEGER NOT NULL,
    amount REAL NOT NULL,
    status TEXT CHECK(status IN ('pending', 'released')) DEFAULT 'pending',
    FOREIGN KEY (employer_id) REFERENCES users(id),
    FOREIGN KEY (freelancer_id) REFERENCES users(id),
    FOREIGN KEY (job_id) REFERENCES jobs(id),
    FOREIGN KEY (milestone_id) REFERENCES milestones(id)
)
''')

# Commit and close connection
conn.commit()
conn.close()

print("Database and tables created successfully!")