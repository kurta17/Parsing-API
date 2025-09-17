import uvicorn


def main():
    uvicorn.run("orders_server.main:app", host="127.0.0.1", port=8000, log_level="info")


if __name__ == "__main__":
    main()
