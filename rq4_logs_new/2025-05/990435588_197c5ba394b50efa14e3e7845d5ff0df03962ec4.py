from flask import Flask, render_template, redirect, url_for

app = Flask(__name__)

# Rozšířený seznam knih (14 položek celkem)
knihy = [
    {"nazev": "Sto roků samoty", "autor": "Gabriel García Márquez", "dostupna": True},
    {"nazev": "1984", "autor": "George Orwell", "dostupna": False},
    {"nazev": "Pán prstenů", "autor": "J.R.R. Tolkien", "dostupna": True},
    {"nazev": "Jméno větru", "autor": "Patrick Rothfuss", "dostupna": False},
    {"nazev": "Zločin a trest", "autor": "Fjodor Michajlovič Dostojevskij", "dostupna": True},
    {"nazev": "Velký Gatsby", "autor": "F. Scott Fitzgerald", "dostupna": True},
    {"nazev": "Na větrné hůrce", "autor": "Emily Brontëová", "dostupna": False},
    {"nazev": "Malý princ", "autor": "Antoine de Saint-Exupéry", "dostupna": True},
    {"nazev": "Duna", "autor": "Frank Herbert", "dostupna": False},
    {"nazev": "Pýcha a předsudek", "autor": "Jane Austenová", "dostupna": True},
    {"nazev": "Alchymista", "autor": "Paulo Coelho", "dostupna": True},
    {"nazev": "Harry Potter a Kámen mudrců", "autor": "J.K. Rowlingová", "dostupna": True},
    {"nazev": "Mechanický pomeranč", "autor": "Anthony Burgess", "dostupna": False},
    {"nazev": "Let do nebezpečí", "autor": "Arthur Hailey", "dostupna": True},
]

@app.route("/")
def uvod():
    return redirect(url_for("zobraz_knihy"))

@app.route("/knihy")
def zobraz_knihy():
    return render_template("knihy.html", knihy=knihy)

@app.route("/o-aplikaci")
def o_aplikaci():
    return render_template("o_aplikaci.html")

if __name__ == "__main__":
    app.run(debug=True)