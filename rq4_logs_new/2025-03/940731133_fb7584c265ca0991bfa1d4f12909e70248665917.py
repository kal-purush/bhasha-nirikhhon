from flask import Flask, render_template_string
import sqlite3
import os

app = Flask(__name__)
DB_FILE = "leads.db"

HTML_TEMPLATE = """
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <title>Freelance Leads Dashboard</title>
    <style>
      body { font-family: Arial, sans-serif; margin: 2em; }
      table { border-collapse: collapse; width: 100%; }
      th, td { padding: 0.5em; border: 1px solid #ccc; text-align: left; }
      th { background-color: #f4f4f4; }
    </style>
  </head>
  <body>
    <h1>Freelance Leads Dashboard</h1>
    <table>
      <tr>
        <th>ID</th>
        <th>Platform</th>
        <th>Post ID</th>
        <th>Title</th>
        <th>Content</th>
        <th>Link</th>
        <th>Timestamp</th>
      </tr>
      {% for row in rows %}
      <tr>
        <td>{{ row[0] }}</td>
        <td>{{ row[1] }}</td>
        <td>{{ row[2] }}</td>
        <td>{{ row[3] }}</td>
        <td>{{ row[4][:150] }}{% if row[4]|length > 150 %}...{% endif %}</td>
        <td><a href="{{ row[5] }}" target="_blank">View</a></td>
        <td>{{ row[7] }}</td>
      </tr>
      {% endfor %}
    </table>
  </body>
</html>
"""

def get_leads():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM leads ORDER BY timestamp DESC")
    rows = cursor.fetchall()
    conn.close()
    return rows

@app.route("/")
def index():
    rows = get_leads()
    return render_template_string(HTML_TEMPLATE, rows=rows)

if __name__ == "__main__":
    # Run the dashboard on port 5000
    app.run(debug=True, port=5000)