FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /opt/kalshi-lab

COPY pyproject.toml README.md /opt/kalshi-lab/
COPY harness /opt/kalshi-lab/harness

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir .

ENTRYPOINT ["kalshi-lab"]
