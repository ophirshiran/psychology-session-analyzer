from fastapi import FastAPI


app = FastAPI(title="Upload API")


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}
