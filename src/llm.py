from openai import OpenAI


import os


def get_client() -> OpenAI:
    token = os.environ["GITHUB_TOKEN"]
    endpoint = "https://models.github.ai/inference"

    client = OpenAI(
        base_url=endpoint,
        api_key=token,
    )
    return client
