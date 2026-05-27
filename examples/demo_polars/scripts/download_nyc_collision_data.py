#!/usr/bin/env python
from __future__ import annotations

import argparse
import csv
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import Request, urlopen


BASE_URL = "https://data.cityofnewyork.us/resource"
USER_AGENT = "bolt-pipeliner-demo/1.0"

CRASH_RESOURCE = "h9gi-nx95"
VEHICLE_RESOURCE = "bm4k-52h4"
PERSON_RESOURCE = "f55k-p6yu"

CRASH_COLUMNS = [
    "crash_date",
    "crash_time",
    "borough",
    "zip_code",
    "latitude",
    "longitude",
    "number_of_persons_injured",
    "number_of_persons_killed",
    "number_of_pedestrians_injured",
    "number_of_cyclist_injured",
    "number_of_motorist_injured",
    "contributing_factor_vehicle_1",
    "contributing_factor_vehicle_2",
    "collision_id",
    "vehicle_type_code1",
    "vehicle_type_code2",
]

VEHICLE_COLUMNS = [
    "unique_id",
    "collision_id",
    "vehicle_id",
    "vehicle_type",
    "vehicle_year",
    "vehicle_occupants",
    "driver_sex",
    "pre_crash",
    "point_of_impact",
    "public_property_damage",
    "contributing_factor_1",
    "contributing_factor_2",
]

PERSON_COLUMNS = [
    "unique_id",
    "collision_id",
    "person_id",
    "person_type",
    "person_age",
    "ejection",
    "safety_equipment",
    "ped_role",
    "contributing_factor_1",
    "contributing_factor_2",
    "person_sex",
]


def fetch_csv(resource: str, params: dict[str, str]) -> str:
    url = f"{BASE_URL}/{resource}.csv?{urlencode(params)}"
    request = Request(url, headers={"User-Agent": USER_AGENT})
    with urlopen(request, timeout=120) as response:
        return response.read().decode("utf-8")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def read_collision_ids(path: Path) -> list[str]:
    with path.open(newline="", encoding="utf-8") as f:
        return [row["collision_id"] for row in csv.DictReader(f) if row.get("collision_id")]


def chunks(values: list[str], size: int):
    for start in range(0, len(values), size):
        yield values[start : start + size]


def fetch_related(resource: str, columns: list[str], ids: list[str], out_path: Path) -> None:
    header_written = False
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as out:
        writer = None
        for chunk in chunks(ids, 100):
            where = "collision_id in (" + ",".join(chunk) + ")"
            text = fetch_csv(
                resource,
                {
                    "$select": ",".join(columns),
                    "$where": where,
                    "$limit": "50000",
                },
            )
            rows = list(csv.reader(text.splitlines()))
            if not rows:
                continue
            if writer is None:
                writer = csv.writer(out)
            if not header_written:
                writer.writerow(rows[0])
                header_written = True
            writer.writerows(rows[1:])


def main() -> None:
    parser = argparse.ArgumentParser(description="Download joinable NYC collision CSVs.")
    parser.add_argument("--limit", type=int, default=2500, help="Number of crash rows to seed.")
    parser.add_argument("--start-date", default="2021-01-01T00:00:00")
    parser.add_argument("--out-dir", default="data/raw")
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    crash_path = out_dir / "crashes.csv"
    vehicle_path = out_dir / "vehicles.csv"
    person_path = out_dir / "persons.csv"

    crashes = fetch_csv(
        CRASH_RESOURCE,
        {
            "$select": ",".join(CRASH_COLUMNS),
            "$where": f"crash_date >= '{args.start_date}' and collision_id is not null",
            "$order": "crash_date desc",
            "$limit": str(args.limit),
        },
    )
    write_text(crash_path, crashes)

    ids = read_collision_ids(crash_path)
    fetch_related(VEHICLE_RESOURCE, VEHICLE_COLUMNS, ids, vehicle_path)
    fetch_related(PERSON_RESOURCE, PERSON_COLUMNS, ids, person_path)

    print(f"Wrote {crash_path}, {vehicle_path}, and {person_path}")


if __name__ == "__main__":
    main()
