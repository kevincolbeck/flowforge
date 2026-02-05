#!/usr/bin/env python3
"""
Universal Integrator - Run Script

Start the server with: python run.py
Or with uvicorn: uvicorn src.api.main:app --reload
"""

from dotenv import load_dotenv
import uvicorn

# Load environment variables from .env file
load_dotenv()


def main():
    uvicorn.run(
        "src.api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info",
    )


if __name__ == "__main__":
    main()
