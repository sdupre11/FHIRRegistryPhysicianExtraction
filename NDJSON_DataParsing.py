import json
import glob
from pathlib import Path
from tqdm import tqdm
import pandas as pd
from collections import defaultdict
from dateutil.parser import parse as dt_parse

# STEP 1: Define root directory and output paths
FHIR_ROOT = Path.home() / "OneDrive - APMA" / "XRegistry"
OUTPUT_DIR = FHIR_ROOT.parent / "FHIR_Processed"
OUTPUT_DIR.mkdir(exist_ok=True)
BATCH_OUTPUT_DIR = OUTPUT_DIR / "batches"
BATCH_OUTPUT_DIR.mkdir(exist_ok=True)

FINAL_OUTPUT = OUTPUT_DIR / "practitioner_flat_table_with_locations.csv"

# STEP 1.5: Paranoid mode - abort if batches already exist
existing_batches = list(BATCH_OUTPUT_DIR.glob("practitioner_batch_*.csv"))
if existing_batches:
    raise RuntimeError(
        f"🚨 Found {len(existing_batches)} existing batch file(s) in {BATCH_OUTPUT_DIR}. "
        "Delete or move them before rerunning to avoid overwriting."
    )

# Helper: Normalize location keys for consistent lookups
def normalize_location_key(ref):
    if not ref:
        return None
    ref = ref.strip().lower()
    return ref if ref.startswith("location/") else f"location/{ref}"

# STEP 2: Gather all necessary file paths
practitioner_files = sorted(glob.glob(str(FHIR_ROOT / "Practitioner" / "*" / "*.ndjson")))
encounter_files = sorted(glob.glob(str(FHIR_ROOT / "Encounter" / "*" / "*.ndjson")))
location_files = sorted(glob.glob(str(FHIR_ROOT / "Location" / "*" / "*.ndjson")))

# STEP 3: Build location lookup table
location_lookup = {}

for file in tqdm(location_files, desc="Loading Locations"):
    with open(file, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                print(f"⚠️ Skipping corrupt JSON in file: {file}")
                continue

            loc_id = rec.get("id")
            name = rec.get("name", "")
            address_obj = rec.get("address", {})

            address = ", ".join(filter(None, [
                " ".join(address_obj.get("line", [])),
                address_obj.get("city"),
                address_obj.get("state"),
                address_obj.get("postalCode"),
                address_obj.get("country")
            ])).strip()

            loc_key = normalize_location_key(loc_id)
            full = f"{name} ({address})" if name else address
            location_lookup[loc_key] = full

# STEP 4: Helper to write a batch to CSV
def write_batch(batch, batch_num):
    batch_file = BATCH_OUTPUT_DIR / f"practitioner_batch_{batch_num:04d}.csv"
    df = pd.DataFrame(batch.values())
    df.to_csv(batch_file, index=False)
    print(f"✅ Wrote batch {batch_num} to {batch_file}")

# STEP 5: Extract Practitioner data with batching
practitioner_data = {}
BATCH_SIZE = 100_000
batch_count = 0

for file in tqdm(practitioner_files, desc="Parsing Practitioners"):
    with open(file, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                print(f"⚠️ Skipping corrupt JSON in file: {file}")
                continue

            pid = rec.get("id")
            if not pid:
                continue

            # ✅ Safer name parsing
            name_entry = (rec.get("name") or [{}])[0]
            prefix = " ".join(name_entry.get("prefix", []))
            given = " ".join(name_entry.get("given", []))
            family = name_entry.get("family", "")
            full_name = " ".join(filter(None, [prefix, given, family])).strip() or "Unknown Provider"

            # Telecom
            phone = email = None
            for telecom in rec.get("telecom", []):
                if telecom.get("system") == "phone":
                    phone = telecom.get("value")
                elif telecom.get("system") == "email":
                    email = telecom.get("value")

            # Address
            address_entry = (rec.get("address") or [{}])[0]
            address = ", ".join(filter(None, [
                " ".join(address_entry.get("line", [])),
                address_entry.get("city"),
                address_entry.get("state"),
                address_entry.get("postalCode"),
                address_entry.get("country")
            ])).strip()

            # Organization reference
            organization = rec.get("organization", {}).get("reference")

            practitioner_data[pid] = {
                "provider_id": pid,
                "npi": next((i['value'] for i in rec.get("identifier", [])
                             if i.get("system", "").lower().endswith("npi")), None),
                "name": full_name,
                "phone": phone,
                "email": email,
                "address": address,
                "organization": organization,
                # placeholders for activity
                "first_activity_date": None,
                "first_activity_location": None,
                "first_activity_address": None,
                "last_activity_date": None,
                "last_activity_location": None,
                "last_activity_address": None
            }

            # ✅ Write batch if threshold reached
            if len(practitioner_data) >= BATCH_SIZE:
                write_batch(practitioner_data, batch_count)
                practitioner_data.clear()
                batch_count += 1

# Write any remaining records
if practitioner_data:
    write_batch(practitioner_data, batch_count)
    practitioner_data.clear()
    batch_count += 1

# STEP 6: Process Encounters and track min/max activity per provider
activity_map = {}

for file in tqdm(encounter_files, desc="Parsing Encounters"):
    with open(file, 'r', encoding='utf-8') as f:
        for line in f:
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                print(f"⚠️ Skipping corrupt JSON in file: {file}")
                continue

            period = rec.get("period", {})
            start = period.get("start")
            end = period.get("end")

            if not start and not end:
                print(f"⚠️ Encounter missing period dates in file {file}")
                continue

            locations = rec.get("location", [])
            location_refs = [
                normalize_location_key(loc.get("location", {}).get("reference"))
                for loc in locations if loc.get("location", {}).get("reference")
            ]
            loc_ref = location_refs[0] if location_refs else None

            for participant in rec.get("participant", []):
                actor_ref = participant.get("individual", {}).get("reference", "")
                if actor_ref.startswith("Practitioner/"):
                    pid = actor_ref.split("/")[-1]
                    if not pid:
                        continue

                    # Initialize if not seen
                    if pid not in activity_map:
                        activity_map[pid] = {
                            "first": (start, loc_ref) if start else None,
                            "last": (end, loc_ref) if end else None
                        }
                    else:
                        # Update first activity
                        if start and activity_map[pid]["first"]:
                            if dt_parse(start) < dt_parse(activity_map[pid]["first"][0]):
                                activity_map[pid]["first"] = (start, loc_ref)
                        elif start:
                            activity_map[pid]["first"] = (start, loc_ref)

                        # Update last activity
                        if end and activity_map[pid]["last"]:
                            if dt_parse(end) > dt_parse(activity_map[pid]["last"][0]):
                                activity_map[pid]["last"] = (end, loc_ref)
                        elif end:
                            activity_map[pid]["last"] = (end, loc_ref)

# STEP 7: Merge batches and enrich with activity data incrementally
# Clear final output if exists
if FINAL_OUTPUT.exists():
    FINAL_OUTPUT.unlink()

batch_files = sorted(BATCH_OUTPUT_DIR.glob("practitioner_batch_*.csv"))

for batch_file in tqdm(batch_files, desc="Merging and Enriching Batches"):
    df = pd.read_csv(batch_file)

    # Vectorized enrichment
    df["first_activity_date"] = df["provider_id"].map(
        lambda pid: activity_map.get(pid, {}).get("first", (None, None))[0]
    )
    df["first_activity_location"] = df["provider_id"].map(
        lambda pid: activity_map.get(pid, {}).get("first", (None, None))[1]
    )
    df["first_activity_address"] = df["first_activity_location"].map(
        lambda loc: location_lookup.get(loc)
    )
    df["last_activity_date"] = df["provider_id"].map(
        lambda pid: activity_map.get(pid, {}).get("last", (None, None))[0]
    )
    df["last_activity_location"] = df["provider_id"].map(
        lambda pid: activity_map.get(pid, {}).get("last", (None, None))[1]
    )
    df["last_activity_address"] = df["last_activity_location"].map(
        lambda loc: location_lookup.get(loc)
    )

    # Deduplicate provider_id within batch (just in case)
    df.drop_duplicates(subset="provider_id", inplace=True)

    # Append enriched batch to final output
    header = not FINAL_OUTPUT.exists()
    df.to_csv(FINAL_OUTPUT, mode="a", index=False, header=header)
    print(f"✅ Appended enriched batch {batch_file.name} to final output")

print(f"🎉 Final output written to {FINAL_OUTPUT}")
