from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.market import router as market_router
from src.api.investment import router as investment_router
from src.api.chat import router as chat_router

app = FastAPI(
    title="Crypto Sentiment Backend API",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(market_router, prefix="/api/market")
app.include_router(investment_router, prefix="/api/investment")
app.include_router(chat_router, prefix="/api/chat")

@app.get("/")
def root():
    return {"status": "ok"}
