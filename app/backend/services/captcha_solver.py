import os
import requests
import time
import json

class CaptchaSolver:
    """Solve Google reCAPTCHA v2 using 2Captcha (or compatible) service.
    The API key is provided via environment variable CAPTCHA_API_KEY or directly
    through the constructor.
    """

    def __init__(self, api_key: str = None):
        self.api_key = api_key or os.getenv("CAPTCHA_API_KEY")
        if not self.api_key:
            raise ValueError("CAPTCHA API key not provided")
        self.solve_url = "http://2captcha.com/in.php"
        self.result_url = "http://2captcha.com/res.php"

    def solve_recaptcha(self, site_key: str, page_url: str, timeout: int = 120) -> str:
        """Submit a reCAPTCHA v2 solving request and poll for the result.
        Returns the token to be injected into the page form.
        """
        payload = {
            "key": self.api_key,
            "method": "userrecaptcha",
            "googlekey": site_key,
            "pageurl": page_url,
            "json": 1,
        }
        resp = requests.post(self.solve_url, data=payload, timeout=30)
        data = resp.json()
        if data.get("status") != 1:
            raise RuntimeError(f"Captcha submit failed: {data.get('request')}")
        request_id = data["request"]
        # Poll for result
        for _ in range(int(timeout / 5)):
            time.sleep(5)
            poll = requests.get(self.result_url, params={
                "key": self.api_key,
                "action": "get",
                "id": request_id,
                "json": 1,
            }, timeout=30)
            result = poll.json()
            if result.get("status") == 1:
                return result["request"]
            if result.get("request") != "CAPCHA_NOT_READY":
                raise RuntimeError(f"Captcha solve error: {result.get('request')}")
        raise TimeoutError("Captcha solving timed out")
