"""CRISP HTTP service entry point.

Exposes three endpoints:

    GET  /health                          — liveness probe
    POST /v2/trace/analysis/stream        — analyse a single time window
    POST /v2/trace/analysis/compare       — compare two time windows

Both analysis endpoints accept a length-prefixed protobuf stream (see
crisp/service/trace_processor.py for the wire format) and return a
serialised crisp.proto.analyzer_pb2.AnalyzeResponse in the response body.

Usage
-----
    # Install the [server] extra:
    #   pip install 'crisp-trace[server]'

    python -m crisp.server           # default port 6566
    python -m crisp.server --port 8080
"""
import argparse
import logging
import sys
import traceback

from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse

from crisp.proto import analyzer_pb2
from crisp.service import streaming

logger = logging.getLogger(__name__)

app = FastAPI(
    title="CRISP",
    description="Critical-path analysis of distributed Jaeger traces.",
    version="0.1.0",
)


# ── endpoints ─────────────────────────────────────────────────────────────────


@app.get("/health")
def health() -> JSONResponse:
    return JSONResponse({"status": "OK"})


@app.post("/v2/trace/analysis/stream")
async def trace_analysis_stream(request: Request) -> Response:
    """Analyse traces from a single time window.

    Request body: length-prefixed AnalyzeRequest protobuf stream
        1. First message  — AnalyzeRequest { metadata: StreamMetadata }
        2. N messages     — AnalyzeRequest { traces: AnalysisData }

    Response body: serialised AnalyzeResponse (application/octet-stream)
    """
    try:
        response = await streaming.process_trace_stream(request)
        return Response(
            content=response.SerializeToString(),
            media_type="application/octet-stream",
        )
    except Exception as exc:
        logger.error("Error in /v2/trace/analysis/stream: %s\n%s", exc, traceback.format_exc())
        return Response(
            content=analyzer_pb2.AnalyzeResponse().SerializeToString(),
            media_type="application/octet-stream",
        )


@app.post("/v2/trace/analysis/compare")
async def trace_analysis_compare(request: Request) -> Response:
    """Compare traces from two time windows (baseline vs comparison).

    Request body: same wire format as /stream, but AnalysisData messages
        carry a ComparisonType field to separate the two windows.

    Response body: serialised AnalyzeResponse with report_window_1,
        report_window_2, and report_window_diff populated.
    """
    try:
        response = await streaming.process_trace_comparison(request)
        return Response(
            content=response.SerializeToString(),
            media_type="application/octet-stream",
        )
    except Exception as exc:
        logger.error("Error in /v2/trace/analysis/compare: %s\n%s", exc, traceback.format_exc())
        return Response(
            content=analyzer_pb2.AnalyzeResponse().SerializeToString(),
            media_type="application/octet-stream",
        )


# ── entry point ───────────────────────────────────────────────────────────────


def main(argv=None) -> int:
    try:
        import uvicorn
    except ImportError:
        print(
            "uvicorn is not installed. Run:  pip install 'crisp-trace[server]'",
            file=sys.stderr,
        )
        return 1

    parser = argparse.ArgumentParser(description="CRISP HTTP service")
    parser.add_argument("--host", default="0.0.0.0", help="Bind host (default 0.0.0.0)")
    parser.add_argument("--port", type=int, default=6566, help="Bind port (default 6566)")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    )
    logger.info("Starting CRISP HTTP service on %s:%d", args.host, args.port)

    uvicorn.run(
        app,
        host=args.host,
        port=args.port,
        log_config=None,
        access_log=False,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
