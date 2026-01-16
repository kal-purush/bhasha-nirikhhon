from flask import Flask, request

app = Flask(__name__)


@app.route("/", methods=['GET'])
def welcome():
    form = """
    <form action="/" method=GET>
        <label>Witaj!<br>Pomyśl o liczbie od 0 do 1000,<br>
               a ja spróbuję odgadnąć ją w 10-ciu podejściach!
        </label><br>
        <button type="submit" formaction="/zgadywanie">Gramy!</button>
    </form>
    """

    return form


@app.route("/zgadywanie", methods=['GET', 'POST'])
def zgadywanie():
    formularz = """
    <form action="/zgadywanie" method=POST>
        <input type="hidden" name="i" value={}>
        <input type="hidden" name="min" value={}>
        <input type="hidden" name="max" value={}>
        <label name="guess">Zgaduję: {}</label><br>
        <button type="submit" name="answer" value="ok">Trafiłeś</button>
        <button type="submit" name="answer" value="more">Więcej</button>
        <button type="submit" name="answer" value="less">Mniej</button>
    </form>
    """

    if request.method == 'POST':
        max = int(request.form['max'])       # pobieram max
        min = int(request.form['min'])       # pobieram min
        guess = int((max - min) / 2) + min   # obliczam guess
        i = int(request.form['i']) + 1       # pobieram oraz zwiększam iterator
        answer = request.form['answer']      # pobieram odpowiedź

        if answer == "ok":
            return "Wygrałem! :D<br>Gramy jeszcze raz?" + \
                   formularz.format(str(0), str(0), str(1000), str(500))
        elif answer == "more":
            min = guess
        elif answer == "less":
            max = guess

        guess = int((max - min) / 2) + min

        if i == 10:
            return "Przegrałem :(<br>Gramy od nowa?" + \
                   formularz.format(str(0), str(0), str(1000), str(500))

        return formularz.format(str(i), str(min), str(max), str(guess))

    elif request.method == 'GET':
        # ustalenie warunków początkowych
        min = 0
        max = 1000
        guess = int((max - min) / 2) + min
        i = 0
        return formularz.format(str(i), str(min), str(max), str(guess))


app.run(debug=True)