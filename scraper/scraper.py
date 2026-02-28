#!/usr/bin/env python3

import argparse
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.resources import Resource
from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import OTLPMetricExporter
from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader

BASE_URL = "https://pong.qfwfq.org/sacrec/rr"
STATE_FILE = Path.home() / ".pong-metrics-state.json"
DELAY = 0.5  # seconds between requests


def parse_page(html: str, event_id: int) -> dict | None:
    soup = BeautifulSoup(html, "lxml")

    title_tag = soup.find("title")
    if not title_tag:
        return None

    # Date from second <h1>
    h1s = soup.find_all("h1")
    date_str = None
    location = None
    for h1 in h1s:
        if h1.get("class") and "locdef" in h1["class"]:
            location = h1.get_text(strip=True)
        else:
            text = h1.get_text(strip=True)
            if re.match(r"\d{4}-\d{2}-\d{2}", text):
                date_str = text[:10]

    if not date_str:
        return None

    event_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    timestamp_ns = int(event_date.timestamp() * 1e9)

    players = []
    tables = soup.find_all("table", class_="bordered")

    # Separate division tables from highlights
    division_tables = []
    highlights = {}
    for table in tables:
        caption = table.find("caption")
        if not caption:
            continue
        caption_text = caption.get_text(strip=True)
        if caption_text.startswith("Table"):
            match = re.search(r"Table\s+(\d+)", caption_text)
            table_num = int(match.group(1)) if match else 0
            division_tables.append((table_num, table))
        elif caption_text == "Highlights":
            for row in table.find_all("tr"):
                header = row.find("td", class_="sideheader")
                value = row.find("td", class_="la")
                if header and value:
                    key = header.get_text(strip=True)
                    val = value.get_text(strip=True)
                    highlights[key] = val

    for table_num, table in division_tables:
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all("td")
            if not cells:
                continue

            # First cell is old rating
            old_rating_text = cells[0].get_text(strip=True)
            if not old_rating_text.isdigit():
                continue

            old_rating = int(old_rating_text)

            # Second cell is name with link
            name_cell = cells[1]
            link = name_cell.find("a")
            if not link:
                continue
            player_name = link.get_text(strip=True)
            href = link.get("href", "")
            player_id_match = re.search(r"/profile/(\d+)", href)
            player_id = player_id_match.group(1) if player_id_match else "0"

            # Count wins and losses from result cells
            wins = 0
            losses = 0
            for cell in cells:
                cls = cell.get("class", [])
                if "res" in cls:
                    text = cell.get_text(strip=True)
                    if text == "W":
                        wins += 1
                    elif text == "L":
                        losses += 1

            # Record cell (W-L) - second to last before rank and new rating
            # Last three ra cells: record, rank, new_rating
            ra_cells = [c for c in cells if "ra" in (c.get("class") or [])]
            if len(ra_cells) < 3:
                continue

            rank_text = ra_cells[-2].get_text(strip=True)
            new_rating_text = ra_cells[-1].get_text(strip=True)

            rank = int(rank_text) if rank_text.isdigit() else 0
            new_rating = int(new_rating_text) if new_rating_text.isdigit() else old_rating

            players.append({
                "name": player_name,
                "player_id": player_id,
                "table": table_num,
                "rank": rank,
                "old_rating": old_rating,
                "new_rating": new_rating,
                "rating_change": new_rating - old_rating,
                "wins": wins,
                "losses": losses,
            })

    player_count = 0
    mean_rating = 0.0
    if "Number of Players" in highlights:
        try:
            player_count = int(highlights["Number of Players"])
        except ValueError:
            pass
    if "Mean Rating" in highlights:
        try:
            mean_rating = float(highlights["Mean Rating"])
        except ValueError:
            pass

    return {
        "event_id": event_id,
        "date": date_str,
        "location": location,
        "timestamp_ns": timestamp_ns,
        "players": players,
        "player_count": player_count,
        "mean_rating": mean_rating,
    }


def scrape_page(event_id: int) -> dict | None:
    url = f"{BASE_URL}/{event_id}"
    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code == 404:
            print(f"  Page {event_id}: 404, skipping")
            return None
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  Page {event_id}: error {e}")
        return None
    return parse_page(resp.text, event_id)


def detect_latest() -> int:
    """Find the latest event ID by checking the main page for navigation links."""
    # Start from a known high number and probe forward
    last_known = load_state()
    if last_known == 0:
        last_known = 1170  # reasonable starting point

    current = last_known
    while True:
        url = f"{BASE_URL}/{current}"
        try:
            resp = requests.get(url, timeout=30)
            if resp.status_code == 404:
                return current - 1
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "lxml")
            # Check if "Next" is a link or plain text
            next_link = None
            for a in soup.find_all("a"):
                if a.get_text(strip=True) == "Next":
                    next_link = a
                    break
            if next_link is None:
                # "Next" exists as plain text -- this is the last page
                return current
            current += 1
            time.sleep(DELAY)
        except requests.RequestException:
            return current - 1


def load_state() -> int:
    if STATE_FILE.exists():
        data = json.loads(STATE_FILE.read_text())
        return data.get("last_scraped", 0)
    return 0


def save_state(last_id: int):
    STATE_FILE.write_text(json.dumps({"last_scraped": last_id}))


def emit_metrics(events: list[dict], endpoint: str):
    from opentelemetry.proto.common.v1.common_pb2 import AnyValue, KeyValue
    from opentelemetry.proto.metrics.v1.metrics_pb2 import (
        Gauge,
        Metric,
        NumberDataPoint,
        ResourceMetrics,
        ScopeMetrics,
    )
    from opentelemetry.proto.resource.v1.resource_pb2 import Resource as PBResource
    from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import (
        ExportMetricsServiceRequest,
    )
    from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2_grpc import (
        MetricsServiceStub,
    )
    import grpc

    channel = grpc.insecure_channel(endpoint)
    stub = MetricsServiceStub(channel)

    resource = PBResource(
        attributes=[
            KeyValue(key="service.name", value=AnyValue(string_value="pong-scraper")),
        ]
    )

    # Build metrics per event in batches
    BATCH_SIZE = 50
    for batch_start in range(0, len(events), BATCH_SIZE):
        batch = events[batch_start : batch_start + BATCH_SIZE]
        metrics = []

        for event in batch:
            eid = str(event["event_id"])
            ts = event["timestamp_ns"]

            # Event-level metrics
            metrics.append(
                Metric(
                    name="pong.event.player_count",
                    gauge=Gauge(
                        data_points=[
                            NumberDataPoint(
                                as_int=event["player_count"],
                                time_unix_nano=ts,
                                attributes=[
                                    KeyValue(key="event_id", value=AnyValue(string_value=eid)),
                                ],
                            )
                        ]
                    ),
                )
            )
            metrics.append(
                Metric(
                    name="pong.event.mean_rating",
                    gauge=Gauge(
                        data_points=[
                            NumberDataPoint(
                                as_double=event["mean_rating"],
                                time_unix_nano=ts,
                                attributes=[
                                    KeyValue(key="event_id", value=AnyValue(string_value=eid)),
                                ],
                            )
                        ]
                    ),
                )
            )

            # Player-level metrics
            for p in event["players"]:
                attrs = [
                    KeyValue(key="player", value=AnyValue(string_value=p["name"])),
                    KeyValue(key="player_id", value=AnyValue(string_value=p["player_id"])),
                    KeyValue(key="event_id", value=AnyValue(string_value=eid)),
                ]

                for metric_name, value, is_int in [
                    ("pong.player.rating", p["new_rating"], True),
                    ("pong.player.rating_change", p["rating_change"], True),
                    ("pong.player.wins", p["wins"], True),
                    ("pong.player.losses", p["losses"], True),
                    ("pong.player.table", p["table"], True),
                    ("pong.player.rank", p["rank"], True),
                ]:
                    dp_kwargs = {
                        "time_unix_nano": ts,
                        "attributes": list(attrs),
                    }
                    if is_int:
                        dp_kwargs["as_int"] = value
                    else:
                        dp_kwargs["as_double"] = value

                    metrics.append(
                        Metric(
                            name=metric_name,
                            gauge=Gauge(data_points=[NumberDataPoint(**dp_kwargs)]),
                        )
                    )

        request = ExportMetricsServiceRequest(
            resource_metrics=[
                ResourceMetrics(
                    resource=resource,
                    scope_metrics=[
                        ScopeMetrics(
                            metrics=metrics,
                        )
                    ],
                )
            ]
        )

        try:
            stub.Export(request, timeout=30)
            print(f"  Exported batch of {len(batch)} events ({len(metrics)} metric points)")
        except grpc.RpcError as e:
            print(f"  gRPC export error: {e.code()} {e.details()}")

    channel.close()


def main():
    parser = argparse.ArgumentParser(description="Scrape pong stats and emit OTel metrics")
    parser.add_argument("--start", type=int, help="Start event ID")
    parser.add_argument("--end", type=int, help="End event ID")
    parser.add_argument("--latest", action="store_true", help="Scrape only new pages since last run")
    parser.add_argument("--endpoint", default="localhost:4317", help="OTel collector gRPC endpoint")
    parser.add_argument("--dry-run", action="store_true", help="Parse only, don't emit metrics")
    args = parser.parse_args()

    if args.latest:
        start = load_state() + 1
        end = detect_latest()
        if start > end:
            print("No new pages to scrape")
            return
        print(f"Scraping new pages: {start} to {end}")
    elif args.start is not None and args.end is not None:
        start = args.start
        end = args.end
    else:
        parser.error("Specify --start/--end or --latest")
        return

    events = []
    for event_id in range(start, end + 1):
        print(f"Scraping event {event_id}/{end}...")
        event = scrape_page(event_id)
        if event:
            print(f"  {event['date']} @ {event['location']}: {len(event['players'])} players")
            events.append(event)
        time.sleep(DELAY)

    if not events:
        print("No events scraped")
        return

    print(f"\nScraped {len(events)} events total")

    if args.dry_run:
        for e in events:
            print(f"  Event {e['event_id']}: {e['date']}, {len(e['players'])} players")
        return

    print(f"Pushing metrics to {args.endpoint}...")
    emit_metrics(events, args.endpoint)

    save_state(end)
    print(f"Done. State saved (last_scraped={end})")


if __name__ == "__main__":
    main()
