from __future__ import annotations

from datetime import datetime

import pandas as pd


def _vehicle_type_group(vehicle: pd.Series) -> pd.Series:
    grouped = pd.Series("other", index=vehicle.index, dtype="string")
    grouped[vehicle.str.contains("TAXI|LIVERY|FOR HIRE", na=False)] = "taxi"
    grouped[vehicle.str.contains("BUS", na=False)] = "bus"
    grouped[vehicle.str.contains("TRUCK|PICK-UP|VAN|TRACTOR|DUMP", na=False)] = "truck_van"
    grouped[vehicle.str.contains("MOTORCYCLE|MOPED|SCOOTER|BIKE|BICYCLE", na=False)] = "two_wheeler"
    grouped[vehicle.str.contains("SPORT UTILITY|SUV|STATION WAGON", na=False)] = "suv_wagon"
    grouped[vehicle.str.contains("SEDAN|PASSENGER", na=False)] = "passenger_car"
    return grouped


def process_data(self, input_tables):
    vehicles = input_tables["vehicles"].copy()
    vehicles["vehicle_occupants"] = pd.to_numeric(vehicles["vehicle_occupants"], errors="coerce").fillna(0).astype("Int64")

    vehicle_type = vehicles["vehicle_type"].fillna("UNKNOWN")
    vehicles["vehicle_type_group"] = _vehicle_type_group(vehicle_type)

    current_year = datetime.now().year
    vehicle_year = pd.to_numeric(vehicles["vehicle_year"], errors="coerce")
    sane_year = vehicle_year.between(1980, current_year + 1, inclusive="both")
    vehicles["vehicle_age"] = pd.Series(pd.NA, index=vehicles.index, dtype="Float64")
    vehicles.loc[sane_year.fillna(False), "vehicle_age"] = (
        current_year - vehicle_year[sane_year.fillna(False)]
    ).astype(float)

    for col, value in {
        "driver_sex": "UNKNOWN",
        "pre_crash": "UNKNOWN",
        "point_of_impact": "UNKNOWN",
        "public_property_damage": "N",
        "contributing_factor_1": "UNSPECIFIED",
        "contributing_factor_2": "UNSPECIFIED",
    }.items():
        vehicles[col] = vehicles[col].fillna(value)

    return vehicles
