# main.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# app.api.routersからメインのAPIルーターをインポート
# app.core.configから設定オブジェクトをインポート
# これらは先に作成しておく必要があります
from app.api.routers import api_router  # app/api/routers.py で定義される想定
from app.core.config import settings    # app/core/config.py で定義される想定

# FastAPIアプリケーションインスタンスの作成
# FastAPIの引数でドキュメントのURLなどをカスタマイズできます
# 例: docs_url="/docs", redoc_url="/redoc"
app = FastAPI(
    title=settings.PROJECT_NAME,
    openapi_url=f"{settings.API_V1_STR}/openapi.json" # OpenAPIスキーマのパス
)

# CORS (Cross-Origin Resource Sharing) ミドルウェアの設定
# フロントエンドが異なるオリジンからAPIにアクセスする場合に必要
if settings.BACKEND_CORS_ORIGINS:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[str(origin) for origin in settings.BACKEND_CORS_ORIGINS], # settings.BACKEND_CORS_ORIGINS を適切に設定
        allow_credentials=True,
        allow_methods=["*"], # GET, POST, PUT, DELETEなど許可するメソッド
        allow_headers=["*"], # 許可するヘッダー
    )

# APIルーターをアプリケーションに含める
# settings.API_V1_STR で定義されたプレフィックス（例: "/api/v1"）を付ける
app.include_router(api_router, prefix=settings.API_V1_STR)

# (オプション) アプリケーション起動時のイベントハンドラ
@app.on_event("startup")
async def startup_event():
    """
    アプリケーション起動時に実行する処理
    例: データベース接続の初期化、必要なリソースの読み込みなど
    """
    print("Application startup...")
    # ここに起動時処理を記述
    # 例:
    # from app.db.session import engine, Base
    # Base.metadata.create_all(bind=engine) # SQLAlchemyの場合のテーブル作成など
    pass

# (オプション) アプリケーション終了時のイベントハンドラ
@app.on_event("shutdown")
async def shutdown_event():
    """
    アプリケーション終了時に実行する処理
    例: データベース接続のクローズなど
    """
    print("Application shutdown...")
    # ここに終了時処理を記述
    pass

# ルートパスへの簡単なエンドポイント (動作確認用)
@app.get("/", tags=["Root"])
async def read_root():
    """
    ルートエンドポイント。アプリケーションが正常に動作しているか確認できます。
    """
    return {"message": f"Welcome to {settings.PROJECT_NAME}!"}

# Uvicornで実行するための設定 (主に開発時)
# コマンドラインから `uvicorn main:app --reload` で起動する場合、この部分は不要
if __name__ == "__main__":
    import uvicorn
    # host="0.0.0.0" で外部からのアクセスを許可
    # port=8000 はFastAPIのデフォルト。必要に応じて変更
    # reload=True でコード変更時に自動リロード（開発時のみ推奨）
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)