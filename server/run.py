import sys
import uvicorn
from config import get_settings


def main():
    settings = get_settings()
    prod     = "--prod" in sys.argv
    uvicorn.run(
        "app.main:app",
        host=settings.host,
        port=settings.port,
        reload=not prod,
        workers=4 if prod else 1,
        log_level="warning",
        access_log=False,
    )


if __name__ == "__main__":
    main()
