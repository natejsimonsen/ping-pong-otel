#!/usr/bin/env python3

import argparse
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://pong.qfwfq.org/sacrec/rr"
STATE_FILE = Path.home() / ".pong-metrics-state.json"
DATA_DIR = Path(__file__).parent.parent / "data"


def parse_page(html: str, event_id: int) -> dict | None:
    soup = BeautifulSoup(html, "lxml")

    title_tag = soup.find("title")
    if not title_tag:
        return None

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

            old_rating_text = cells[0].get_text(strip=True)
            if not old_rating_text.isdigit():
                continue

            old_rating = int(old_rating_text)

            name_cell = cells[1]
            link = name_cell.find("a")
            if not link:
                continue
            player_name = link.get_text(strip=True)
            href = link.get("href", "")
            player_id_match = re.search(r"/profile/(\d+)", href)
            player_id = player_id_match.group(1) if player_id_match else "0"

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
            return None
        resp.raise_for_status()
    except requests.RequestException as e:
        print(f"  Page {event_id}: error {e}")
        return None
    return parse_page(resp.text, event_id)


def scrape_range(start: int, end: int, workers: int) -> list[dict]:
    events = []
    ids = list(range(start, end + 1))
    done = 0
    total = len(ids)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(scrape_page, eid): eid for eid in ids}
        for future in as_completed(futures):
            done += 1
            eid = futures[future]
            try:
                event = future.result()
            except Exception as e:
                print(f"  Page {eid}: exception {e}")
                continue
            if event:
                events.append(event)
                if done % 50 == 0 or done == total:
                    print(f"  {done}/{total} pages scraped, {len(events)} events parsed")

    events.sort(key=lambda e: e["event_id"])
    return events


def detect_latest() -> int:
    last_known = load_state()
    if last_known == 0:
        last_known = 1170

    current = last_known
    while True:
        url = f"{BASE_URL}/{current}"
        try:
            resp = requests.get(url, timeout=30)
            if resp.status_code == 404:
                return current - 1
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "lxml")
            next_link = None
            for a in soup.find_all("a"):
                if a.get_text(strip=True) == "Next":
                    next_link = a
                    break
            if next_link is None:
                return current
            current += 1
            time.sleep(0.5)
        except requests.RequestException:
            return current - 1


def load_state() -> int:
    if STATE_FILE.exists():
        data = json.loads(STATE_FILE.read_text())
        return data.get("last_scraped", 0)
    return 0


def save_state(last_id: int):
    STATE_FILE.write_text(json.dumps({"last_scraped": last_id}))


def load_events(path: Path) -> list[dict]:
    data = json.loads(path.read_text())
    if isinstance(data, list):
        return data
    return data.get("events", [])


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

    BATCH_SIZE = 50
    for batch_start in range(0, len(events), BATCH_SIZE):
        batch = events[batch_start : batch_start + BATCH_SIZE]
        metrics = []

        for event in batch:
            eid = str(event["event_id"])
            ts = event["timestamp_ns"]

            event_attrs = [
                KeyValue(key="location", value=AnyValue(string_value=event.get("location") or "")),
            ]

            metrics.append(
                Metric(
                    name="pong.event.player_count",
                    gauge=Gauge(
                        data_points=[
                            NumberDataPoint(
                                as_int=event["player_count"],
                                time_unix_nano=ts,
                                attributes=list(event_attrs),
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
                                attributes=list(event_attrs),
                            )
                        ]
                    ),
                )
            )

            for p in event["players"]:
                attrs = [
                    KeyValue(key="player", value=AnyValue(string_value=p["name"])),
                    KeyValue(key="player_id", value=AnyValue(string_value=p["player_id"])),
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
            print(f"  Exported batch {batch_start // BATCH_SIZE + 1}: {len(batch)} events, {len(metrics)} metrics")
        except grpc.RpcError as e:
            print(f"  gRPC export error: {e.code()} {e.details()}")

    channel.close()


def cmd_scrape(args):
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
        print("Specify --start/--end or --latest")
        return

    print(f"Scraping {end - start + 1} pages with {args.workers} workers...")
    events = scrape_range(start, end, args.workers)

    if not events:
        print("No events scraped")
        return

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    out_path = DATA_DIR / f"events_{start}_{end}.json"
    out_path.write_text(json.dumps(events, indent=2))
    print(f"Saved {len(events)} events to {out_path}")

    save_state(end)


def cmd_push(args):
    path = Path(args.file)
    if not path.exists():
        print(f"File not found: {path}")
        return

    events = load_events(path)
    print(f"Loaded {len(events)} events from {path}")

    if args.start or args.end:
        events = [
            e for e in events
            if (args.start is None or e["event_id"] >= args.start)
            and (args.end is None or e["event_id"] <= args.end)
        ]
        print(f"Filtered to {len(events)} events")

    if not events:
        print("No events to push")
        return

    print(f"Pushing to {args.endpoint}...")
    emit_metrics(events, args.endpoint)
    print("Done")


def cmd_validate(args):
    path = Path(args.file)
    if not path.exists():
        print(f"File not found: {path}")
        return

    events = load_events(path)
    total_players = sum(len(e["players"]) for e in events)
    dates = [e["date"] for e in events]

    print(f"Events: {len(events)}")
    print(f"Date range: {min(dates)} to {max(dates)}")
    print(f"Total player-event rows: {total_players}")

    if args.player:
        matches = []
        for e in events:
            for p in e["players"]:
                if args.player.lower() in p["name"].lower():
                    matches.append((e["date"], e["event_id"], p))
        print(f"\nMatches for '{args.player}': {len(matches)}")
        for date, eid, p in matches[-10:]:
            print(f"  {date} (#{eid}): {p['name']} rating={p['new_rating']} ({p['rating_change']:+d}) {p['wins']}W-{p['losses']}L table {p['table']}")


def main():
    parser = argparse.ArgumentParser(description="Pong metrics scraper")
    sub = parser.add_subparsers(dest="command", required=True)

    # scrape
    p_scrape = sub.add_parser("scrape", help="Scrape pages and save to JSON")
    p_scrape.add_argument("--start", type=int)
    p_scrape.add_argument("--end", type=int)
    p_scrape.add_argument("--latest", action="store_true")
    p_scrape.add_argument("--workers", type=int, default=8, help="Parallel workers (default 8)")

    # push
    p_push = sub.add_parser("push", help="Push JSON data to OTel collector")
    p_push.add_argument("file", help="Path to events JSON file")
    p_push.add_argument("--endpoint", default="localhost:4317")
    p_push.add_argument("--start", type=int, help="Filter: min event ID")
    p_push.add_argument("--end", type=int, help="Filter: max event ID")

    # validate
    p_validate = sub.add_parser("validate", help="Validate and inspect JSON data")
    p_validate.add_argument("file", help="Path to events JSON file")
    p_validate.add_argument("--player", help="Filter by player name")

    args = parser.parse_args()
    if args.command == "scrape":
        cmd_scrape(args)
    elif args.command == "push":
        cmd_push(args)
    elif args.command == "validate":
        cmd_validate(args)


if __name__ == "__main__":
    main()
