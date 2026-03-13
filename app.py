import os
from datetime import datetime, timezone

import boto3


def _required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _mask_value(name: str, value: str | None) -> str:
    if not value:
        return "<missing>"
    if name in {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"}:
        if len(value) <= 8:
            return "*" * len(value)
        return f"{value[:4]}...{value[-4:]}"
    return value


def _env_report_lines() -> list[str]:
    env_names = [
        "AWS_BUCKET_NAME",
        "AWS_S3_ENDPOINT",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_SESSION_TOKEN",
        "OUTPUT_PREFIX",
        "OUTPUT_FILE_NAME",
        "HELLO_MESSAGE",
    ]

    lines = ["environment_report:"]
    for name in env_names:
        value = os.environ.get(name)
        exists = "yes" if value else "no"
        lines.append(f"{name}: exists={exists}; value={_mask_value(name, value)}")
    return lines


def main() -> None:
    bucket_name = _required_env("AWS_BUCKET_NAME")
    endpoint_host = _required_env("AWS_S3_ENDPOINT")
    access_key = _required_env("AWS_ACCESS_KEY_ID")
    secret_key = _required_env("AWS_SECRET_ACCESS_KEY")
    session_token = os.environ.get("AWS_SESSION_TOKEN")

    output_prefix = os.environ.get("OUTPUT_PREFIX", "hello-process-test").strip("/")
    file_name = os.environ.get("OUTPUT_FILE_NAME", "hello.txt")
    message = os.environ.get("HELLO_MESSAGE", "hello world")
    timestamp = datetime.now(timezone.utc).isoformat()

    endpoint_url = (
        endpoint_host
        if endpoint_host.startswith("http://") or endpoint_host.startswith("https://")
        else f"https://{endpoint_host}"
    )

    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=session_token,
    )

    object_key = f"{output_prefix}/{file_name}" if output_prefix else file_name
    report = "\n".join(_env_report_lines())
    body = f"{message}\ncreated_at_utc={timestamp}\n{report}\n"

    s3_client.put_object(
        Bucket=bucket_name,
        Key=object_key,
        Body=body.encode("utf-8"),
        ContentType="text/plain; charset=utf-8",
    )

    print(f"Wrote s3://{bucket_name}/{object_key}")
    print(body, end="")


if __name__ == "__main__":
    main()
