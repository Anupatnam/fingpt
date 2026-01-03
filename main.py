from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import threading

from src.api.market import router as market_router
from src.api.investment import router as investment_router
from src.api.chat import router as chat_router

from src.ingestion.coinbase_ws import start_ws
from src.processing.aggregator import start_aggregator

app = FastAPI(
    title="Crypto Sentiment Backend API",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(market_router, prefix="/api/market")
app.include_router(investment_router, prefix="/api/investment")
app.include_router(chat_router, prefix="/api/chat")

@app.on_event("startup")
def startup():
    print("🚀 Starting ingestion + aggregation")

    threading.Thread(
        target=start_ws,
        daemon=True
    ).start()

    threading.Thread(
        target=start_aggregator,
        daemon=True
    ).start()

@app.get("/")
def root():
    return {"status": "ok", "message": "Backend live on Railway"}
