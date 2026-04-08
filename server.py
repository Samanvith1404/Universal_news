from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pathlib import Path
import json

app = FastAPI()

# ------------------ FRONTEND ------------------

# serve static files (HTML, CSS, JS)
app.mount("/static", StaticFiles(directory="static"), name="static")

# main page
@app.get("/")
def home():
    return FileResponse("static/index.html")


# ------------------ PHARMA DATA ------------------

def load_pharma(modality: str):
    file_path = Path(f"News_portals/brief_{modality}.json")

    if not file_path.exists():
        return None

    try:
        return json.loads(file_path.read_text(encoding="utf-8"))
    except Exception:
        return None


@app.get("/api/pharma/{modality}")
def get_pharma(modality: str):
    data = load_pharma(modality)

    if not data:
        raise HTTPException(status_code=404, detail="Pharma data not found")

    return data


# ------------------ COMPANY DATA ------------------

def load_company():
    file_path = Path("companies_news/briefs.json")

    if not file_path.exists():
        return None

    try:
        return json.loads(file_path.read_text(encoding="utf-8"))
    except Exception:
        return None


@app.get("/api/company/{modality}")
def get_company(modality: str):
    data = load_company()

    if not data or "news" not in data:
        raise HTTPException(status_code=404, detail="Company data not found")

    news_items = data["news"]

    # 🔥 FILTER BY MODALITY
    filtered = [
        item for item in news_items
        if item.get("modality", "").lower() == modality.replace("_", " ").lower()
    ]

    return {
        "article_count": len(filtered),
        "sections": [
            {
                "heading": f"{modality.replace('_',' ').title()} Company News",
                "points": [
                    {
                        "text": f"{item.get('company', '')}: {item.get('news', '')}",
                        "url": item.get("url")
                    }
                    for item in filtered
                ]
            }
        ],
        "generated_at": data.get("meta", {}).get("generated_at", ""),
        "query": modality
    }


# ------------------ HEALTH CHECK ------------------

@app.get("/health")
def health():
    return {"status": "running"}
