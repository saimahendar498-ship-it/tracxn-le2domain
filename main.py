from __future__ import annotations

from waitress import serve

from app.api import create_app
from app.config import get_settings


def main() -> None:
    settings = get_settings()
    app = create_app(settings, start_workers=True)
    print(f"Open http://{settings.host}:{settings.port}/")
    serve(app, host=settings.host, port=settings.port, threads=32)


if __name__ == "__main__":
    main()
