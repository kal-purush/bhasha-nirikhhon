from flask import Flask, request, render_template

app = Flask(__name__)

@app.route('/signup', methods=['POST', 'GET'])
def Signup():
    if request.method == 'POST':
        user_name = request.form.get("username")
        password = request.form.get("password")
        return f"Hello {user_name}"
    return render_template("signup.html")
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)