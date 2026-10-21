"""Generate synthetic carrier tracking files - with the defects real carriers send.

    python scripts/generate_sample_data.py

Writes one CSV per carrier per week to ``data/landing/carrier=<CODE>/`` plus
``data/samples/malformed_header.csv`` (a file the ingest Lambda should
quarantine). Deterministic: the same seed always produces the same files.
"""

from __future__ import annotations

import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1] / "data"
FIELDS = [
    "event_id",
    "shipment_id",
    "carrier_code",
    "event_type",
    "event_ts",
    "location_code",
    "customer_id",
    "weight_kg",
    "declared_value",
]
CARRIERS = {"SWFT": (20, 40), "NRTH": (40, 110), "BLUE": (30, 80), "PKGO": (60, 150)}
HUBS = ["NYC1", "CHI2", "DAL1", "ATL3", "DEN1", "LAX2", "SEA1", "MIA1", "PHX1", "BOS2"]
ALIASES = {
    "PICKED_UP": ["PICKED_UP", "Pickup", "picked up"],
    "IN_TRANSIT": ["IN_TRANSIT", "in transit", "TRANSIT"],
    "OUT_FOR_DELIVERY": ["OUT_FOR_DELIVERY", "OFD", "Out For Delivery"],
    "DELIVERED": ["DELIVERED", "Delivered", "DLVD"],
}
START = datetime(2026, 9, 1, 6, 0)


def ts(rng: random.Random, moment: datetime) -> str:
    if rng.random() < 0.06:
        return moment.strftime("%m/%d/%Y %H:%M")
    return moment.strftime("%Y-%m-%dT%H:%M:%S" if rng.random() < 0.8 else "%Y-%m-%d %H:%M:%S")


def shipment_events(rng: random.Random, n: int, carrier: str) -> list[dict]:
    low, high = CARRIERS[carrier]
    created = START + timedelta(hours=rng.randint(0, 26 * 24))
    origin, dest = rng.sample(HUBS, 2)
    weight = round(rng.uniform(0.3, 40), 2)
    value = round(rng.uniform(15, 2500), 2)
    base = {
        "shipment_id": f"SHP-{carrier}-{n:06d}",
        "carrier_code": carrier if rng.random() > 0.05 else carrier.lower(),
        "customer_id": f"CUST-{rng.randint(1, 300):04d}",
        "weight_kg": f"{weight}kg" if rng.random() < 0.1 else str(weight),
        "declared_value": f"${value:,.2f}" if rng.random() < 0.15 else str(value),
    }
    steps = [
        ("CREATED", created, origin),
        ("PICKED_UP", created + timedelta(hours=rng.randint(1, 8)), origin),
    ]
    transit_hours = rng.uniform(low, high)
    for i in range(rng.randint(1, 3)):
        steps.append(
            ("IN_TRANSIT", steps[1][1] + timedelta(hours=transit_hours * (i + 1) / 4), rng.choice(HUBS))
        )
    if rng.random() < 0.08:
        steps.append(("EXCEPTION", steps[-1][1] + timedelta(hours=3), steps[-1][2]))
        transit_hours += rng.uniform(12, 48)
    if rng.random() < 0.9:  # ~10% still in flight at extract time
        delivered = steps[1][1] + timedelta(hours=transit_hours)
        steps.append(("OUT_FOR_DELIVERY", delivered - timedelta(hours=rng.randint(2, 6)), dest))
        steps.append(("DELIVERED", delivered, dest))

    rows = []
    for i, (event, moment, location) in enumerate(steps):
        rows.append(
            {
                **base,
                "event_id": f"EVT-{carrier}-{n:06d}-{i:02d}",
                "event_type": rng.choice(ALIASES.get(event, [event])),
                "event_ts": ts(rng, moment),
                "location_code": location,
                "_week": n % 4,  # spread shipments over 4 weekly drops (not written)
            }
        )
    return rows


def inject_defects(rng: random.Random, rows: list[dict]) -> list[dict]:
    defects = [
        lambda r: r.update(event_id=""),
        lambda r: r.update(event_ts="2026-13-45T99:00:00"),
        lambda r: r.update(weight_kg="-4"),
        lambda r: r.update(event_type="TELEPORTED"),
        lambda r: r.update(shipment_id=" "),
    ]
    for row in rng.sample(rows, len(rows) // 60):
        rng.choice(defects)(row)
    resent = [dict(r) for r in rng.sample(rows, len(rows) // 40)]  # carriers re-send events
    return rows + resent


def main(seed: int = 7) -> None:
    rng = random.Random(seed)
    landing = ROOT / "landing"
    total = 0
    for carrier in CARRIERS:
        rows = [e for n in range(1, 151) for e in shipment_events(rng, n, carrier)]
        rows = inject_defects(rng, rows)
        weeks: dict[int, list[dict]] = {}
        for row in rows:
            weeks.setdefault(row["_week"], []).append(row)
        for week, chunk in sorted(weeks.items()):
            path = landing / f"carrier={carrier}" / f"events_2026-09-W{week + 1}.csv"
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("w", newline="", encoding="utf-8") as fh:
                writer = csv.DictWriter(fh, fieldnames=FIELDS, extrasaction="ignore")
                writer.writeheader()
                writer.writerows(chunk)
            total += len(chunk)
    samples = ROOT / "samples"
    samples.mkdir(parents=True, exist_ok=True)
    (samples / "malformed_header.csv").write_text(
        "id,shipment,carrier,status,time\n1,SHP-1,SWFT,DELIVERED,2026-09-01\n", encoding="utf-8"
    )
    print(f"Wrote {total:,} events for {len(CARRIERS)} carriers to {landing}")


if __name__ == "__main__":
    main()
