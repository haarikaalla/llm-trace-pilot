"""
detector_worker.py — ML detection microservice.

Every DETECTOR_INTERVAL_SEC it loads the recent window from the log index,
runs all detectors, routes anomalies through the AlertManager (alerts,
incident grouping), enriches new incidents with RCA + AI summary and
publishes to the anomalies/alerts/incidents topics.

Run:  python -m observability.services.detector_worker
"""
from __future__ import annotations

import logging
import os
import signal
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from observability.config import get_settings
from observability.services.platform import ObservabilityPlatform

log = logging.getLogger("detector-worker")


def main() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(name)s %(message)s")
    s = get_settings()
    platform = ObservabilityPlatform(s, inline=False)
    running = True

    def stop(*_):
        nonlocal running
        running = False
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    log.info("detector started interval=%ds window=%ds threshold=%.2f", s.detector_interval_sec,
             s.detection_window_sec, s.anomaly_threshold)
    while running:
        started = time.time()
        try:
            res = platform.run_detection()
            log.info("scanned=%d anomalies=%d alerts=%d incidents=%d resolved=%d", res["events_scanned"],
                     len(res["anomalies"]), len(res["alerts"]), len(res["incidents"]), len(res["resolved"]))
        except Exception:                               # noqa: BLE001
            log.exception("detection cycle failed")
        time.sleep(max(0.5, s.detector_interval_sec - (time.time() - started)))
    platform.bus.close()


if __name__ == "__main__":
    main()
