import os
from http.server import HTTPServer, SimpleHTTPRequestHandler, test

from dotenv import load_dotenv

from .merge_files import suffix

load_dotenv()


class CORSRequestHandler(SimpleHTTPRequestHandler):
    def end_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Access-Control-Allow-Headers", "ngrok-skip-browser-warning")
        self.send_header("Content-Encoding", suffix)
        self.send_header("Content-Type", "application/json")
        super().end_headers()

    def do_OPTIONS(self):
        self.send_response(200)
        self.end_headers()


if __name__ == "__main__":
    serve_path = os.environ["OA_ROOT"] + "/pruned-cache"
    print(f"serving {serve_path}")

    pre_dir = os.getcwd()
    os.chdir(serve_path)
    try:
        test(CORSRequestHandler, HTTPServer, port=8000)
    except KeyboardInterrupt:
        pass
    os.chdir(pre_dir)
