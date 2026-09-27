"""Synthetic CMS-shaped data for offline runs, CI and demos.

The generator reproduces the *shape* of the real CMS Care Compare extracts -
identical column names, string-typed values, CMS sentinel values such as
"Not Available", footnotes, and the "Better/No Different/Worse Than the
National Rate" vocabulary - so every downstream layer is exercised exactly as
it would be against the live API.

Hospitals, names and scores are fabricated. A latent "quality" score per
hospital drives the star rating, the measure comparisons and the patient
survey stars so the dashboard shows realistic correlations.

It also injects the kinds of defects real feeds contain (duplicates,
malformed ids, unknown states, bad numerics) so the quarantine and data
quality layers have something to catch.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from functools import lru_cache

# Two quarterly releases -> lets the pipeline demonstrate backfill, change
# detection and SCD Type 2 history on hospital attributes.
SAMPLE_RELEASES = ["2026-04-22", "2026-07-22"]

# CMS Certification Number (CCN) state prefixes
STATE_CODES = {
    "AL": "01", "AK": "02", "AZ": "03", "AR": "04", "CA": "05", "CO": "06", "CT": "07",
    "DE": "08", "DC": "09", "FL": "10", "GA": "11", "HI": "12", "ID": "13", "IL": "14",
    "IN": "15", "IA": "16", "KS": "17", "KY": "18", "LA": "19", "ME": "20", "MD": "21",
    "MA": "22", "MI": "23", "MN": "24", "MS": "25", "MO": "26", "MT": "27", "NE": "28",
    "NV": "29", "NH": "30", "NJ": "31", "NM": "32", "NY": "33", "NC": "34", "ND": "35",
    "OH": "36", "OK": "37", "OR": "38", "PA": "39", "PR": "40", "RI": "41", "SC": "42",
    "SD": "43", "TN": "44", "TX": "45", "UT": "46", "VT": "47", "VA": "49", "WA": "50",
    "WV": "51", "WI": "52", "WY": "53",
}
# Rough relative hospital counts per state (weights for sampling)
STATE_WEIGHTS = {
    "CA": 34, "TX": 38, "FL": 22, "NY": 17, "PA": 19, "IL": 19, "OH": 19, "GA": 15,
    "NC": 11, "MI": 14, "NJ": 7, "VA": 9, "WA": 9, "AZ": 9, "MA": 7, "TN": 12, "IN": 12,
    "MO": 12, "MD": 5, "WI": 13, "CO": 9, "MN": 13, "SC": 7, "AL": 9, "LA": 13, "KY": 10,
    "OR": 6, "OK": 12, "CT": 3, "UT": 5, "IA": 12, "NV": 4, "AR": 8, "MS": 9, "KS": 13,
    "NM": 4, "NE": 9, "ID": 4, "WV": 5, "HI": 2, "NH": 3, "ME": 3, "MT": 6, "RI": 1,
    "DE": 1, "SD": 6, "ND": 4, "AK": 2, "DC": 1, "VT": 1, "WY": 3, "PR": 5,
}
# State-level quality offset so the choropleth has real geographic signal
STATE_EFFECT = {s: v for s, v in zip(
    sorted(STATE_CODES), [((i * 37) % 17 - 8) / 20 for i in range(len(STATE_CODES))], strict=True)}

HOSPITAL_TYPES = [
    ("Acute Care Hospitals", 0.62),
    ("Critical Access Hospitals", 0.25),
    ("Psychiatric", 0.05),
    ("Childrens", 0.02),
    ("Acute Care - Veterans Administration", 0.04),
    ("Rural Emergency Hospital", 0.02),
]
OWNERSHIP = [
    ("Voluntary non-profit - Private", 0.42),
    ("Proprietary", 0.20),
    ("Voluntary non-profit - Other", 0.09),
    ("Voluntary non-profit - Church", 0.07),
    ("Government - Hospital District or Authority", 0.09),
    ("Government - Local", 0.06),
    ("Government - State", 0.03),
    ("Physician", 0.02),
    ("Tribal", 0.02),
]
OWNERSHIP_EFFECT = {"Proprietary": -0.15, "Physician": 0.25, "Government - State": -0.1}

NAME_PREFIX = ["Riverside", "Summit", "Valley", "Lakeview", "Mercy", "St. Anne", "Cedar",
               "Pioneer", "Harbor", "Northside", "Grandview", "Prairie", "Bayfront", "Sierra",
               "Highland", "Maple", "Oak Ridge", "Crescent", "Evergreen", "Redwood", "Granite",
               "Willow Creek", "Sunrise", "Liberty", "Heritage", "Canyon", "Clearwater"]
NAME_SUFFIX = ["Regional Medical Center", "Community Hospital", "Memorial Hospital",
               "General Hospital", "Health Center", "Medical Center", "County Hospital"]
CITIES = ["Fairview", "Springfield", "Franklin", "Greenville", "Clinton", "Madison",
          "Georgetown", "Salem", "Arlington", "Ashland", "Milton", "Newport", "Oxford",
          "Riverton", "Jackson", "Burlington", "Dayton", "Lexington", "Marion", "Bristol"]

# (measure_id, measure_name, national_rate, spread) - lower score is better
COMPLICATION_MEASURES = [
    ("COMP_HIP_KNEE", "Rate of complications for hip/knee replacement patients", 3.2, 0.6),
    ("MORT_30_AMI", "Death rate for heart attack patients", 12.4, 1.3),
    ("MORT_30_CABG", "Death rate for CABG surgery patients", 3.0, 0.7),
    ("MORT_30_COPD", "Death rate for COPD patients", 8.9, 1.1),
    ("MORT_30_HF", "Death rate for heart failure patients", 11.6, 1.5),
    ("MORT_30_PN", "Death rate for pneumonia patients", 17.4, 2.0),
    ("MORT_30_STK", "Death rate for stroke patients", 13.6, 1.4),
    ("Hybrid_HWM", "Hybrid Hospital-Wide All-Cause Risk Standardized Mortality Rate", 4.6, 0.7),
    ("PSI_03_ULCER", "Pressure ulcer rate", 0.55, 0.3),
    ("PSI_04_SURG_COMP", "Death rate among surgical inpatients with serious treatable complications", 162.0, 18.0),
    ("PSI_06_IAT_PTX", "Iatrogenic pneumothorax rate", 0.23, 0.06),
    ("PSI_09_POST_HEM", "Postoperative hemorrhage or hematoma rate", 2.4, 0.4),
    ("PSI_11_POST_RESP", "Postoperative respiratory failure rate", 8.1, 1.6),
    ("PSI_12_POSTOP_PULMEMB_DVT", "Perioperative pulmonary embolism or deep vein thrombosis rate", 3.5, 0.6),
    ("PSI_13_POST_SEPSIS", "Postoperative sepsis rate", 4.6, 0.9),
    ("PSI_90", "Patient safety and adverse events composite", 1.0, 0.12),
]
UNPLANNED_MEASURES = [
    ("READM_30_AMI", "Acute Myocardial Infarction (AMI) 30-Day Readmission Rate", 14.1, 1.0),
    ("READM_30_CABG", "Rate of readmission for CABG", 10.9, 1.1),
    ("READM_30_COPD", "Rate of readmission for chronic obstructive pulmonary disease (COPD) patients", 18.6, 1.2),
    ("READM_30_HF", "Heart failure (HF) 30-Day Readmission Rate", 20.1, 1.5),
    ("READM_30_HIP_KNEE", "Rate of readmission after hip/knee replacement", 4.4, 0.6),
    ("READM_30_PN", "Pneumonia (PN) 30-Day Readmission Rate", 16.3, 1.2),
    ("Hybrid_HWR", "Hybrid Hospital-Wide All-Cause Readmission Measure (HWR)", 14.5, 0.9),
    ("EDAC_30_AMI", "Hospital return days for heart attack patients", 0.0, 22.0),
    ("EDAC_30_HF", "Hospital return days for heart failure patients", 0.0, 25.0),
    ("EDAC_30_PN", "Hospital return days for pneumonia patients", 0.0, 20.0),
    ("OP_32", "Rate of unplanned hospital visits after colonoscopy (per 1,000 colonoscopies)", 15.8, 2.2),
    ("OP_35_ADM", "Rate of inpatient admissions for patients receiving outpatient chemotherapy", 10.2, 1.4),
    ("OP_35_ED", "Rate of emergency department (ED) visits for patients receiving outpatient chemotherapy", 5.1, 1.0),
    ("OP_36", "Ratio of unplanned hospital visits after hospital outpatient surgery", 1.0, 0.12),
]
HCAHPS_STAR_MEASURES = [
    ("H_COMP_1_STAR_RATING", "Nurse communication - star rating"),
    ("H_COMP_2_STAR_RATING", "Doctor communication - star rating"),
    ("H_COMP_3_STAR_RATING", "Staff responsiveness - star rating"),
    ("H_COMP_5_STAR_RATING", "Communication about medicines - star rating"),
    ("H_COMP_6_STAR_RATING", "Discharge information - star rating"),
    ("H_COMP_7_STAR_RATING", "Care transition - star rating"),
    ("H_CLEAN_STAR_RATING", "Cleanliness - star rating"),
    ("H_QUIET_STAR_RATING", "Quietness - star rating"),
    ("H_HSP_RATING_STAR_RATING", "Overall hospital rating - star rating"),
    ("H_RECMND_STAR_RATING", "Recommend hospital - star rating"),
    ("H_STAR_RATING", "Summary star rating"),
]
HCAHPS_PCT_MEASURES = [
    ("H_COMP_1_A_P", 'Patients who reported that their nurses "Always" communicated well',
     'Nurses "always" communicated well', 80),
    ("H_CLEAN_HSP_A_P", 'Patients who reported that their room and bathroom were "Always" clean',
     'Room was "always" clean', 72),
    ("H_HSP_RATING_9_10", "Patients who gave their hospital a rating of 9 or 10 on a scale from 0 (lowest) to 10 (highest)",
     "Patients who gave a rating of 9 or 10 (high)", 71),
    ("H_RECMND_DY", 'Patients who reported YES, they would definitely recommend the hospital',
     '"YES", patients would definitely recommend the hospital', 70),
]

GEO_COLS = ["facility_id", "facility_name", "address", "citytown", "state", "zip_code",
            "countyparish", "telephone_number"]


@dataclass
class _Hospital:
    facility_id: str
    name: str
    address: str
    city: str
    state: str
    zip_code: str
    county: str
    phone: str
    hospital_type: str
    ownership: str
    emergency: str
    birthing: str
    quality: float  # latent, ~N(0,1); higher is better
    volume: int


def _weighted(rng: random.Random, options: list[tuple[str, float]]) -> str:
    return rng.choices([o for o, _ in options], weights=[w for _, w in options])[0]


def _build_hospitals(rng: random.Random, n: int) -> list[_Hospital]:
    states = list(STATE_WEIGHTS)
    weights = list(STATE_WEIGHTS.values())
    used: set[str] = set()
    out: list[_Hospital] = []
    while len(out) < n:
        st = rng.choices(states, weights=weights)[0]
        fid = f"{STATE_CODES[st]}{rng.randint(1, 899):04d}"
        if fid in used:
            continue
        used.add(fid)
        htype = _weighted(rng, HOSPITAL_TYPES)
        own = _weighted(rng, OWNERSHIP)
        if htype == "Acute Care - Veterans Administration":
            own = "Veterans Health Administration"
        city = rng.choice(CITIES)
        quality = rng.gauss(0, 1) + STATE_EFFECT[st] + OWNERSHIP_EFFECT.get(own, 0)
        out.append(_Hospital(
            facility_id=fid,
            name=f"{rng.choice(NAME_PREFIX)} {rng.choice(NAME_SUFFIX)}".upper(),
            address=f"{rng.randint(100, 9999)} {rng.choice(['MAIN', 'OAK', 'CENTER', 'HOSPITAL', 'PARK'])} "
                    f"{rng.choice(['STREET', 'AVENUE', 'DRIVE', 'BOULEVARD'])}",
            city=city.upper(),
            state=st,
            zip_code=f"{rng.randint(501, 99950):05d}",
            county=rng.choice(["JEFFERSON", "WASHINGTON", "FRANKLIN", "LINCOLN", "MADISON",
                               "JACKSON", "MONROE", "GREENE", "UNION", "CLAY"]),
            phone=f"({rng.randint(201, 989)}) {rng.randint(200, 999)}-{rng.randint(1000, 9999)}",
            hospital_type=htype,
            ownership=own,
            emergency="Yes" if htype != "Psychiatric" and rng.random() < 0.93 else "No",
            birthing="Y" if htype == "Acute Care Hospitals" and rng.random() < 0.55 else "",
            quality=quality,
            volume=int(rng.lognormvariate(5.5 if htype == "Acute Care Hospitals" else 3.8, 0.7)),
        ))
    return out


def _star_from_quality(q: float, rng: random.Random) -> int:
    z = q + rng.gauss(0, 0.45)
    cuts = [-1.3, -0.45, 0.35, 1.2]
    return 1 + sum(z > c for c in cuts)


def _geo(h: _Hospital) -> dict:
    return {"facility_id": h.facility_id, "facility_name": h.name, "address": h.address,
            "citytown": h.city, "state": h.state, "zip_code": h.zip_code,
            "countyparish": h.county, "telephone_number": h.phone}


def _hospital_info_row(h: _Hospital, rng: random.Random) -> dict:
    rated = h.hospital_type in ("Acute Care Hospitals", "Acute Care - Veterans Administration") \
        or (h.hospital_type == "Critical Access Hospitals" and rng.random() < 0.35)
    # hospital-seeded noise: the rating only moves when latent quality drifts across a cut-point
    rating = str(_star_from_quality(h.quality, random.Random(h.facility_id))) if rated else "Not Available"
    row = _geo(h) | {
        "hospital_type": h.hospital_type,
        "hospital_ownership": h.ownership,
        "emergency_services": h.emergency,
        "meets_criteria_for_birthing_friendly_designation": h.birthing,
        "hospital_overall_rating": rating,
        "hospital_overall_rating_footnote": "" if rated else "16",
    }
    for grp, n in (("mort", 7), ("safety", 8), ("readm", 11)):
        if rated:
            better = max(0, int(rng.gauss(max(h.quality, 0) * 1.2, 0.8)))
            worse = max(0, int(rng.gauss(max(-h.quality, 0) * 1.2, 0.8)))
            better, worse = min(better, n), min(worse, n - min(better, n))
            row |= {f"{grp}_group_measure_count": str(n),
                    f"count_of_facility_{grp}_measures": str(n),
                    f"count_of_{grp}_measures_better": str(better),
                    f"count_of_{grp}_measures_no_different": str(n - better - worse),
                    f"count_of_{grp}_measures_worse": str(worse),
                    f"{grp}_group_footnote": ""}
        else:
            row |= {f"{grp}_group_measure_count": str(n),
                    f"count_of_facility_{grp}_measures": "Not Available",
                    f"count_of_{grp}_measures_better": "Not Available",
                    f"count_of_{grp}_measures_no_different": "Not Available",
                    f"count_of_{grp}_measures_worse": "Not Available",
                    f"{grp}_group_footnote": "5"}
    return row


def _measure_rows(h: _Hospital, rng: random.Random, measures, period, is_unplanned: bool) -> list[dict]:
    rows = []
    small = h.volume < 40
    for mid, mname, nat, spread in measures:
        if h.hospital_type in ("Psychiatric", "Rural Emergency Hospital"):
            continue
        base = _geo(h) | {"measure_id": mid, "measure_name": mname, "start_date": period[0],
                          "end_date": period[1], "footnote": ""}
        if is_unplanned:
            base |= {"number_of_patients": "", "number_of_patients_returned": ""}
        if small and rng.random() < 0.7:
            rows.append(base | {"compared_to_national": "Number of Cases Too Small",
                                "denominator": "Not Available", "score": "Not Available",
                                "lower_estimate": "Not Available", "higher_estimate": "Not Available",
                                "footnote": "1"})
            continue
        denom = max(25, int(h.volume * rng.uniform(0.2, 1.6)))
        score = nat - h.quality * spread * 0.55 + rng.gauss(0, spread * 0.6)
        if nat > 0:
            score = max(score, nat * 0.2)
        half_width = spread * 1.6 * (150 / (denom + 150)) ** 0.5 + spread * 0.25
        lo, hi = score - half_width, score + half_width
        if mid.startswith("EDAC"):
            verdict = ("Fewer Days Than Average per 100 Discharges" if hi < nat else
                       "More Days Than Average per 100 Discharges" if lo > nat else
                       "Average Days per 100 Discharges")
        else:
            verdict = ("Better Than the National Rate" if hi < nat else
                       "Worse Than the National Rate" if lo > nat else
                       "No Different Than the National Rate")
        row = base | {"compared_to_national": verdict, "denominator": str(denom),
                      "score": f"{score:.1f}" if spread >= 0.3 else f"{score:.2f}",
                      "lower_estimate": f"{lo:.1f}" if spread >= 0.3 else f"{lo:.2f}",
                      "higher_estimate": f"{hi:.1f}" if spread >= 0.3 else f"{hi:.2f}"}
        if is_unplanned and mid.startswith(("READM", "EDAC")):
            pts = int(denom * rng.uniform(0.85, 1.0))
            row |= {"number_of_patients": str(pts),
                    "number_of_patients_returned": str(int(pts * max(score, 5) / 100))}
        rows.append(row)
    return rows


def _hcahps_rows(h: _Hospital, rng: random.Random, period) -> list[dict]:
    rows = []
    surveyed = h.hospital_type in ("Acute Care Hospitals", "Acute Care - Veterans Administration") or (
        h.hospital_type == "Critical Access Hospitals" and h.volume > 60)
    n_surveys = str(int(h.volume * rng.uniform(0.8, 3.5))) if surveyed else "Not Available"
    rr = str(rng.randint(12, 32)) if surveyed else "Not Available"
    common = {"number_of_completed_surveys": n_surveys, "number_of_completed_surveys_footnote": "",
              "survey_response_rate_percent": rr, "survey_response_rate_percent_footnote": "",
              "start_date": period[0], "end_date": period[1], "hcahps_linear_mean_value": "Not Applicable"}
    for mid, q in HCAHPS_STAR_MEASURES:
        hosp_rng = random.Random(f"{h.facility_id}-{mid}")
        star = str(_star_from_quality(h.quality * 0.8 + hosp_rng.gauss(0, 0.5), hosp_rng)) if surveyed else "Not Available"
        rows.append(_geo(h) | common | {
            "hcahps_measure_id": mid, "hcahps_question": q, "hcahps_answer_description": q,
            "patient_survey_star_rating": star, "patient_survey_star_rating_footnote": "" if surveyed else "15",
            "hcahps_answer_percent": "Not Applicable", "hcahps_answer_percent_footnote": ""})
    for mid, q, desc, base_pct in HCAHPS_PCT_MEASURES:
        pct = str(max(30, min(98, int(base_pct + h.quality * 5 + rng.gauss(0, 3))))) if surveyed else "Not Available"
        rows.append(_geo(h) | common | {
            "hcahps_measure_id": mid, "hcahps_question": q, "hcahps_answer_description": desc,
            "patient_survey_star_rating": "Not Applicable", "patient_survey_star_rating_footnote": "",
            "hcahps_answer_percent": pct, "hcahps_answer_percent_footnote": "" if surveyed else "15"})
    return rows


def _periods(release: str) -> dict[str, tuple[str, str]]:
    later = release >= "2026-07-01"
    return {
        "complications_deaths": ("07/01/2022", "06/30/2025") if later else ("04/01/2022", "03/31/2025"),
        "unplanned_visits": ("07/01/2022", "06/30/2025") if later else ("04/01/2022", "03/31/2025"),
        "hcahps": ("04/01/2025", "03/31/2026") if later else ("01/01/2025", "12/31/2025"),
    }


def _inject_defects(rows: list[dict], rng: random.Random, dataset: str) -> list[dict]:
    """Add realistic feed defects: duplicate rows, malformed ids, bad state codes, bad numerics."""
    if not rows:
        return rows
    out = list(rows)
    for r in rng.sample(rows, k=max(1, len(rows) // 250)):   # ~0.4% exact duplicates
        out.append(dict(r))
    for r in rng.sample(out, k=3):                            # malformed CCNs
        r["facility_id"] = rng.choice(["", "N/A", "12AB"])
    if dataset == "hospital_info":
        rng.choice(out)["state"] = "ZZ"                        # unknown state
    elif "score" in out[0]:
        rng.choice(out)["score"] = "12..4"                     # unparseable numeric
    return out


@lru_cache(maxsize=4)
def generate_release(release: str, n_hospitals: int = 2000, seed: int = 42) -> dict[str, list[dict]]:
    """Return {dataset_name: rows} for one CMS release, deterministic for (release, seed)."""
    base_rng = random.Random(seed)
    hospitals = _build_hospitals(base_rng, n_hospitals)

    # Release-specific membership & drift -> openings/closures and attribute changes
    rel_idx = SAMPLE_RELEASES.index(release) if release in SAMPLE_RELEASES else len(SAMPLE_RELEASES)
    rng = random.Random(f"{seed}-{release}")
    active = hospitals[: n_hospitals - 20 + rel_idx * 10]  # 10 new hospitals appear each release
    if rel_idx > 0:
        closed = set(random.Random(seed + 1).sample([h.facility_id for h in active[:n_hospitals - 20]], 6))
        active = [h for h in active if h.facility_id not in closed]
        drift = random.Random(f"{seed}-drift-{release}")
        for h in active:
            h.quality += drift.gauss(0, 0.15)                   # quality drifts between releases
            if drift.random() < 0.01:
                h.ownership = "Proprietary" if h.ownership != "Proprietary" else "Voluntary non-profit - Private"

    periods = _periods(release)
    data = {
        "hospital_info": [_hospital_info_row(h, rng) for h in active],
        "complications_deaths": [r for h in active for r in
                                 _measure_rows(h, rng, COMPLICATION_MEASURES, periods["complications_deaths"], False)],
        "unplanned_visits": [r for h in active for r in
                             _measure_rows(h, rng, UNPLANNED_MEASURES, periods["unplanned_visits"], True)],
        "hcahps": [r for h in active for r in _hcahps_rows(h, rng, periods["hcahps"])],
    }
    return {name: _inject_defects(rows, random.Random(f"{seed}-{release}-{name}"), name)
            for name, rows in data.items()}
