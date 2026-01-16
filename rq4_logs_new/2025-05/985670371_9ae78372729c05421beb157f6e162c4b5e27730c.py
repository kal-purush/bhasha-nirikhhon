from fastapi import FastAPI, Request, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import wikipedia

app = FastAPI()
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/", response_class=HTMLResponse)
def homepage(request: Request):
    return templates.TemplateResponse("home.html", {"request": request})

@app.post("/result", response_class=HTMLResponse)
def show_result(request: Request, query: str = Form(...)):
    result_a = result_b = error_a = error_b = url_b = None

    # Version A – Basic
    try:
        result_a = wikipedia.summary(query, sentences=2)
    except wikipedia.exceptions.PageError:
        error_a = "Page not found."
    except wikipedia.exceptions.DisambiguationError as e:
        error_a = f"Too many possible results. Try being more specific. Options: {', '.join(e.options[:5])}..."
    except Exception as e:
        error_a = f"Unexpected error: {str(e)}"

    # Version B – Improved
    try:
        results = wikipedia.search(query)
        try:
            page = wikipedia.page(query, auto_suggest=False)
        except wikipedia.exceptions.PageError:
            if results:
                page = wikipedia.page(results[0], auto_suggest=False)
            else:
                error_b = "No results found."
        if not error_b:
            result_b = page.summary[:500]
            url_b = page.url
    except wikipedia.exceptions.DisambiguationError as e:
        error_b = f"Too many possible results. Try being more specific. Options: {', '.join(e.options[:5])}..."
    except Exception as e:
        error_b = f"Unexpected error: {str(e)}"

    return templates.TemplateResponse("result.html", {
        "request": request,
        "query": query,
        "result_a": result_a,
        "error_a": error_a,
        "result_b": result_b,
        "error_b": error_b,
        "url_b": url_b
    })

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="127.0.0.1", port=8000, reload=True)