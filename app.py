# app.py
from flask import Flask
from analytics_s3 import analytics_bp

app = Flask(__name__)
app.register_blueprint(analytics_bp)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
