from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
from psycopg2.extras import RealDictCursor

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_HOST     = "caboose.proxy.rlwy.net"
DB_PORT     = 14180
DB_NAME     = "dap_city_db"
DB_USER     = "postgres"
DB_PASSWORD = "SYBcmjqGMhsqhxZNRSAVdiumWTSFaOxG"

def get_connection():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD,
        cursor_factory=RealDictCursor
    )

def get_disability_category(disability_type: str) -> str:
    if not disability_type:
        return "special"
    t = disability_type.lower()
    if any(x in t for x in [
        'type 18', 'type 19', 'type 20',
        'thalassemia', 'hemophilia', 'sickle', 'th)', 'he)', 'scd)'
    ]):
        return "blood"
    if any(x in t for x in [
        'type 11', 'type 12', 'type 13', 'type 14', 'type 15', 'type 16', 'type 17',
        'intellectual', 'autism', 'mental illness', 'learning disab', 'parkinson',
        'sclerosis', 'neurological', 'id/', 'id)', 'mr)',
        'asd)', 'mi)', 'sld)', 'pd)', 'ms)', 'cnc)'
    ]):
        return "cognitive"
    if any(x in t for x in [
        'type 21', 'type 10', 'multiple disab', 'acid attack', 'speech',
        'language disab', 'splad', 'ac)', 'md)'
    ]):
        return "special"
    if any(x in t for x in [
        'type 7', 'type 8', 'blind', 'low vision', 'vi)', 'lv)'
    ]):
        return "visual"
    if any(x in t for x in ['type 9', 'hearing', 'hi)', 'hh)']):
        return "hearing"
    if any(x in t for x in [
        'type 1', 'type 3', 'type 4', 'type 5', 'type 6',
        'locomotor', 'cerebral', 'leprosy', 'muscular',
        'dwarfism', 'ld)', 'cp)', 'lc)', 'mud)', 'df)'
    ]):
        return "motor"
    return "special"


VALID_COORD_FILTER = """
    v.latitude  IS NOT NULL
    AND v.longitude IS NOT NULL
    AND v.latitude::text  ~ '^-?[0-9]+(\\.[0-9]+)?$'
    AND v.longitude::text ~ '^-?[0-9]+(\\.[0-9]+)?$'
    AND v.latitude::float  BETWEEN -90  AND 90
    AND v.longitude::float BETWEEN -180 AND 180
"""

SELECT_FIELDS = """
    v.name,
    REGEXP_REPLACE(v.mobile::text,      '\\.0$', '') AS mobile,
    v.disability_type,
    v.gender,
    v.voter_id,
    v.crew_name,
    REGEXP_REPLACE(v.crew_mobile::text, '\\.0$', '') AS crew_mobile,
    v.pa_block,
    v.pa_taluk,
    v.pa_district,
    REGEXP_REPLACE(v.pa_pincode::text,  '\\.0$', '') AS pa_pincode,
    v.latitude::float  AS latitude,
    v.longitude::float AS longitude,
    COALESCE(c.age::text,  'NA') AS age,
    COALESCE(c.dob::text,  'NA') AS dob
"""

FROM_JOIN = """
    FROM app_dap_voters v
    LEFT JOIN dap_city_voters c ON c.voter_id = v.voter_id
"""

TALUK_GROUP_MAP = {
    'palani':               'Palani',
    'kodaikanal':           'Palani',
    'thoppampatty':         'Oddanchatram',
    'oddanchatram':         'Oddanchatram',
    'reddiyarchatram':      'Athoor',
    'reddiarchatram':       'Athoor',
    'athoor':               'Athoor',
    'batlagundu':           'Nilakottai',
    'nilakottai':           'Nilakottai',
    'natham':               'Natham',
    'shanarpatti':          'Natham',
    'shanarpatty':          'Natham',
    'dindigul':             'Dindigul',
    'vadamadurai':          'Vedasandur',
    'vedasandur':           'Vedasandur',
    'guziliamparai':        'Vedasandur',
    'municipality':             'Municipality',
    'town panchayat':           'Town Panchayat',
    'townpanchayat':            'Town Panchayat',
    'municipal corporations':   'Municipal Corporations',
    'municipal corporation':    'Municipal Corporations',
}

def map_taluk_to_ac(taluk: str) -> str:
    if not taluk:
        return taluk
    return TALUK_GROUP_MAP.get(taluk.lower().strip(), taluk)

def get_matching_taluks(ac_name: str):
    matching = [
        taluk for taluk, mapped in TALUK_GROUP_MAP.items()
        if mapped.lower() == ac_name.lower()
    ]
    if ac_name.lower() not in [t.lower() for t in matching]:
        matching.append(ac_name)
    return matching


@app.get("/debug")
def debug_locations():
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS total FROM app_dap_voters;")
        total = cur.fetchone()
        cur.execute("""
            SELECT COUNT(*) AS with_coords FROM app_dap_voters
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL;
        """)
        with_coords = cur.fetchone()
        cur.execute("""
            SELECT latitude, longitude,
                   pg_typeof(latitude) AS lat_type,
                   pg_typeof(longitude) AS lon_type
            FROM app_dap_voters LIMIT 10;
        """)
        sample = cur.fetchall()
        cur.close()
        conn.close()
        return {
            "total_rows":       dict(total),
            "rows_with_coords": dict(with_coords),
            "sample_rows":      [dict(r) for r in sample],
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/debug-skipped")
def debug_skipped():
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT latitude::text, longitude::text,
                   pg_typeof(latitude) AS lat_type,
                   pg_typeof(longitude) AS lon_type,
                   COUNT(*) AS count
            FROM app_dap_voters
            WHERE latitude IS NOT NULL AND longitude IS NOT NULL
            GROUP BY latitude::text, longitude::text, lat_type, lon_type
            ORDER BY count DESC LIMIT 20;
        """)
        sample = cur.fetchall()
        cur.close()
        conn.close()
        return {"top_coordinate_values": [dict(r) for r in sample]}
    except Exception as e:
        return {"error": str(e)}


@app.get("/debug-ac")
def debug_ac():
    try:
        conn = get_connection()
        cur  = conn.cursor()
        cur.execute("""
            SELECT assembly, COUNT(*) as count
            FROM staging_pwd
            GROUP BY assembly
            ORDER BY assembly;
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return {"assemblies": [dict(r) for r in rows]}
    except Exception as e:
        return {"error": str(e)}


@app.get("/markers")
def get_markers(
    page:  int = Query(default=1,    ge=1),
    limit: int = Query(default=2000, ge=1, le=5000)
):
    offset = (page - 1) * limit
    try:
        conn = get_connection()
        cur  = conn.cursor()
        cur.execute(f"""
            SELECT
                v.voter_id,
                v.disability_type,
                v.latitude::float  AS latitude,
                v.longitude::float AS longitude
            FROM app_dap_voters v
            WHERE {VALID_COORD_FILTER}
            ORDER BY v.voter_id
            LIMIT %s OFFSET %s;
        """, (limit, offset))
        rows = cur.fetchall()
        total_valid = None
        if page == 1:
            cur.execute(f"""
                SELECT COUNT(*) AS total
                FROM app_dap_voters v
                WHERE {VALID_COORD_FILTER};
            """)
            total_valid = cur.fetchone()["total"]
        cur.close()
        conn.close()
        count = len(rows)
        return {
            "page":     page,
            "count":    count,
            "total":    total_valid,
            "has_more": count == limit,
            "data": [
                {
                    "voter_id":  r["voter_id"],
                    "category":  get_disability_category(r["disability_type"] or ""),
                    "latitude":  r["latitude"],
                    "longitude": r["longitude"],
                }
                for r in rows
            ]
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/locations")
def get_locations(
    page:  int = Query(default=1,    ge=1),
    limit: int = Query(default=1000, ge=1, le=5000)
):
    offset = (page - 1) * limit
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(f"""
            SELECT COUNT(*) AS total FROM app_dap_voters v WHERE {VALID_COORD_FILTER};
        """)
        total_valid = cur.fetchone()["total"]
        total_pages = -(-total_valid // limit)
        cur.execute(f"""
            SELECT {SELECT_FIELDS}
            {FROM_JOIN}
            WHERE {VALID_COORD_FILTER}
            ORDER BY v.voter_id LIMIT %s OFFSET %s;
        """, (limit, offset))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        data = [{k: (val if val is not None else "NA") for k, val in row.items()} for row in rows]
        return {
            "page": page, "limit": limit,
            "total_valid_rows": total_valid,
            "total_pages": total_pages,
            "count": len(data), "data": data
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/person/{voter_id}")
def get_person(voter_id: str):
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(f"""
            SELECT {SELECT_FIELDS}
            {FROM_JOIN}
            WHERE v.voter_id = %s LIMIT 1;
        """, (voter_id,))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            return {"error": "Person not found"}
        return {k: (val if val is not None else "NA") for k, val in row.items()}
    except Exception as e:
        return {"error": str(e)}


@app.get("/locations/all")
def get_all_locations():
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(f"""
            SELECT {SELECT_FIELDS}
            {FROM_JOIN}
            WHERE {VALID_COORD_FILTER}
            ORDER BY v.voter_id;
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        data = [{k: (val if val is not None else "NA") for k, val in row.items()} for row in rows]
        return {"total": len(data), "data": data}
    except Exception as e:
        return {"error": str(e)}


@app.get("/ac-list")
def get_ac_list():
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT DISTINCT ac_no, assembly
            FROM staging_pwd
            ORDER BY ac_no;
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return {"data": rows}
    except Exception as e:
        return {"error": str(e)}


@app.get("/ac/{ac_no}")
def get_people_by_ac(ac_no: int, limit: int = 2000):
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT *
            FROM staging_pwd
            WHERE ac_no = %s
            ORDER BY first_name
            LIMIT %s;
        """, (str(ac_no), limit))
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return {"data": rows}
    except Exception as e:
        return {"error": str(e)}


@app.get("/ro-list")
def get_ro_list():
    try:
        conn = get_connection()
        cur  = conn.cursor()
        cur.execute("""
            SELECT
                ro_name, ro_phone, aro_name, aro_phone,
                ac_no, ac_name, blo_name, blo_mobile, part_name
            FROM master_full_data_new
            WHERE ro_name IS NOT NULL
            ORDER BY ro_name, aro_name, part_name;
        """)
        rows = cur.fetchall()
        cur.close()
        conn.close()

        ro_map = {}
        for r in rows:
            ro_key = (r['ro_name'] or '').strip()
            if ro_key not in ro_map:
                ro_map[ro_key] = {
                    'ro_name':   r['ro_name']   or 'NA',
                    'ro_phone':  r['ro_phone']  or 'NA',
                    'aro_name':  r['aro_name']  or 'NA',
                    'aro_phone': r['aro_phone'] or 'NA',
                    'ac_no':     r['ac_no']     or 'NA',
                    'ac_name':   r['ac_name']   or 'NA',
                    'blos': []
                }
            if r['blo_name']:
                ro_map[ro_key]['blos'].append({
                    'blo_name':   r['blo_name']  or 'NA',
                    'blo_mobile': r['blo_mobile'] or 'NA',
                    'part_name':  r['part_name']  or 'NA',
                })
        return {"count": len(ro_map), "data": list(ro_map.values())}
    except Exception as e:
        return {"error": str(e)}


# ── RO queries — now includes row id ─────────────────────────────────────────
@app.get("/ro-queries")
def get_ro_queries(ac_name: str = Query(...)):
    try:
        conn = get_connection()
        cur  = conn.cursor()
        matching_taluks = get_matching_taluks(ac_name)
        if not matching_taluks:
            return {"ac_name": ac_name, "count": 0, "data": []}
        placeholders = ','.join(['%s'] * len(matching_taluks))
        cur.execute(f"""
            SELECT
                id,
                voter_name,
                disability_type,
                taluk,
                will_vote,
                need_assistance,
                assistance_type,
                applied_date::text,
                applied_time::text,
                mobile_number,
                COALESCE(current_status, 'pending') AS current_status
            FROM chatbot_voter_logs
            WHERE LOWER(taluk) IN ({placeholders})
              AND (current_status IS NULL OR current_status != 'checked')
            ORDER BY applied_date DESC, applied_time DESC;
        """, [t.lower() for t in matching_taluks])
        rows = cur.fetchall()
        cur.close()
        conn.close()
        data = [{k: (str(v) if v is not None else 'NA')
                 for k, v in row.items()} for row in rows]
        return {"ac_name": ac_name, "count": len(data), "data": data}
    except Exception as e:
        return {"error": str(e)}


# ── Update status by id (primary key) — no more name+mobile matching ─────────
@app.patch("/ro-queries/update-status")
def update_query_status(row_id: int = Query(..., alias="id")):
    try:
        conn = get_connection()
        cur  = conn.cursor()
        cur.execute("""
            UPDATE chatbot_voter_logs
            SET current_status = 'checked'
            WHERE id = %s
              AND (current_status IS NULL OR current_status != 'checked');
        """, (row_id,))
        updated = cur.rowcount          # 1 = success, 0 = already checked / not found
        conn.commit()
        cur.close()
        conn.close()
        if updated == 0:
            return {"success": False, "message": "Row not found or already checked"}
        return {"success": True, "message": "Status updated to checked"}
    except Exception as e:
        return {"error": str(e)}


@app.get("/ro-queries/assistance-summary")
def get_assistance_summary(ac_name: str = Query(...)):
    try:
        conn = get_connection()
        cur  = conn.cursor()
        matching_taluks = get_matching_taluks(ac_name)
        placeholders = ','.join(['%s'] * len(matching_taluks))
        cur.execute(f"""
            SELECT
                assistance_type,
                COUNT(*) as count
            FROM chatbot_voter_logs
            WHERE LOWER(taluk) IN ({placeholders})
              AND need_assistance = true
              AND (current_status IS NULL OR current_status != 'checked')
            GROUP BY assistance_type
            ORDER BY count DESC;
        """, [t.lower() for t in matching_taluks])
        rows = cur.fetchall()
        cur.close()
        conn.close()
        return {"ac_name": ac_name, "summary": [dict(r) for r in rows]}
    except Exception as e:
        return {"error": str(e)}


@app.post("/blo-login")
def blo_login(blo_mobile: str = Query(...), epic_no: str = Query(...)):
    try:
        conn = get_connection()
        cur  = conn.cursor()
        cur.execute("""
            SELECT blo_name, blo_mobile, ac_no, assembly
            FROM staging_blo
            WHERE blo_mobile = %s AND epic_no = %s
            LIMIT 1;
        """, (blo_mobile, epic_no))
        row = cur.fetchone()
        cur.close()
        conn.close()
        if not row:
            return {"success": False, "error": "Invalid mobile or password"}
        return {
            "success":    True,
            "blo_name":   row["blo_name"]  or "NA",
            "blo_mobile": row["blo_mobile"] or "NA",
            "ac_no":      str(row["ac_no"] or "NA"),
            "assembly":   row["assembly"]  or "NA",
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


# ── BLO queries — now includes row id ────────────────────────────────────────
@app.get("/blo-queries")
def get_blo_queries(blo_mobile: str = Query(...)):
    try:
        conn = get_connection()
        cur  = conn.cursor()

        cur.execute("""
            SELECT ac_no, assembly
            FROM staging_blo
            WHERE blo_mobile = %s
            LIMIT 1;
        """, (blo_mobile,))
        blo_row = cur.fetchone()
        if not blo_row:
            return {"count": 0, "data": []}

        ac_no    = str(blo_row["ac_no"]   or "").strip()
        assembly = str(blo_row["assembly"] or "").strip().lower()

        if not ac_no or not assembly:
            return {"count": 0, "data": []}

        cur.execute("""
            SELECT
                id,
                voter_name,
                disability_type,
                taluk,
                will_vote,
                need_assistance,
                assistance_type,
                applied_date::text,
                applied_time::text,
                mobile_number,
                ps_no,
                COALESCE(current_status, 'pending') AS current_status
            FROM chatbot_voter_logs
            WHERE ps_no::text = %s
              AND LOWER(taluk) = %s
              AND (current_status IS NULL OR current_status != 'checked')
            ORDER BY applied_date DESC, applied_time DESC;
        """, (ac_no, assembly))

        rows = cur.fetchall()
        cur.close()
        conn.close()

        data = [{k: (str(v) if v is not None else 'NA')
                 for k, v in row.items()} for row in rows]
        return {"count": len(data), "data": data}
    except Exception as e:
        return {"error": str(e)}


@app.get("/blo-queries/assistance-summary")
def get_blo_assistance_summary(blo_mobile: str = Query(...)):
    try:
        conn = get_connection()
        cur  = conn.cursor()

        cur.execute("""
            SELECT ac_no, assembly
            FROM staging_blo
            WHERE blo_mobile = %s
            LIMIT 1;
        """, (blo_mobile,))
        blo_row = cur.fetchone()
        if not blo_row:
            return {"summary": []}

        ac_no    = str(blo_row["ac_no"]   or "").strip()
        assembly = str(blo_row["assembly"] or "").strip().lower()

        if not ac_no or not assembly:
            return {"summary": []}

        cur.execute("""
            SELECT assistance_type, COUNT(*) as count
            FROM chatbot_voter_logs
            WHERE ps_no::text = %s
              AND LOWER(taluk) = %s
              AND need_assistance = true
              AND (current_status IS NULL OR current_status != 'checked')
            GROUP BY assistance_type
            ORDER BY count DESC;
        """, (ac_no, assembly))

        rows = cur.fetchall()
        cur.close()
        conn.close()
        return {"summary": [dict(r) for r in rows]}
    except Exception as e:
        return {"error": str(e)}
        


# ── Add this endpoint to your main.py ────────────────────────────────────────
# Table: users   |  mobile column = login number  |  name column = password

@app.post("/super-admin-login")
def super_admin_login(
    mobile:   str = Query(...),
    password: str = Query(...),
):
    """
    Authenticate a super admin.
    DB table : users
    number   → mobile  column
    password → name    column
    """
    try:
        conn = get_connection()
        cur  = conn.cursor()
        cur.execute("""
            SELECT name, mobile
            FROM users
            WHERE mobile = %s
              AND name   = %s
            LIMIT 1;
        """, (mobile, password))
        row = cur.fetchone()
        cur.close()
        conn.close()

        if not row:
            return {"success": False, "error": "Invalid mobile number or password"}

        return {
            "success": True,
            "name":    row["name"]   or "Admin",
            "mobile":  row["mobile"] or mobile,
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

# ── Replace the /admin-dashboard endpoint in your main.py ────────────────────

@app.get("/admin-dashboard")
def get_admin_dashboard():
    """
    Cumulative totals are derived by SUMMING the per-RO numbers,
    so they always match the RO/ARO-wise breakdown exactly.
    """
    try:
        conn = get_connection()
        cur  = conn.cursor()

        # ── 1. All unique ROs ─────────────────────────────────────────────────
        cur.execute("""
            SELECT DISTINCT ro_name, ro_phone, aro_name, aro_phone, ac_name
            FROM master_full_data_new
            WHERE ro_name IS NOT NULL AND TRIM(ro_name) != ''
            ORDER BY ro_name;
        """)
        ro_rows = cur.fetchall()

        # ── 2. Per-taluk base counts ──────────────────────────────────────────
        cur.execute("""
            SELECT
                LOWER(TRIM(taluk))                               AS taluk,
                COUNT(*)                                         AS total_queries,
                COUNT(*) FILTER (WHERE will_vote  = true)        AS will_vote,
                COUNT(*) FILTER (WHERE will_vote  = false)       AS cant_vote,
                COUNT(*) FILTER (WHERE need_assistance = true)   AS need_assistance
            FROM chatbot_voter_logs
            GROUP BY 1;
        """)
        taluk_counts = {r['taluk']: dict(r) for r in cur.fetchall()}

        # ── 3. Per-taluk assistance-type breakdown ───────────────────────────
        cur.execute("""
            SELECT
                LOWER(TRIM(taluk))                                          AS taluk,
                COALESCE(NULLIF(TRIM(assistance_type), ''), 'Unspecified')  AS assistance_type,
                COUNT(*)                                                     AS count
            FROM chatbot_voter_logs
            WHERE need_assistance = true
            GROUP BY 1, 2;
        """)
        taluk_assistance: dict = {}
        for r in cur.fetchall():
            t = r['taluk']
            taluk_assistance.setdefault(t, []).append({
                'assistance_type': r['assistance_type'],
                'count': int(r['count']),
            })

        cur.close()
        conn.close()

        # ── 4. Helper: all taluks that belong to an ac_name ──────────────────
        def get_taluks_for_ac(ac_name: str):
            """Returns every taluk key that maps to this AC (lower-cased)."""
            ac_lower = ac_name.lower().strip()
            matched = [
                taluk.lower().strip()
                for taluk, mapped in TALUK_GROUP_MAP.items()
                if mapped.lower() == ac_lower
            ]
            # Also include the ac_name itself as a direct taluk key
            if ac_lower not in matched:
                matched.append(ac_lower)
            return matched

        # ── 5. Build per-RO breakdown ─────────────────────────────────────────
        ro_breakdown = []
        for ro in ro_rows:
            ac_name = (ro['ac_name'] or '').strip()
            taluks  = get_taluks_for_ac(ac_name)

            ro_total      = 0
            ro_will_vote  = 0
            ro_cant_vote = 0
            ro_need_asst  = 0
            asst_map: dict = {}

            for t in taluks:
                if t in taluk_counts:
                    tc = taluk_counts[t]
                    ro_total     += int(tc.get('total_queries',  0) or 0)
                    ro_will_vote += int(tc.get('will_vote',       0) or 0)
                    ro_cant_vote += int(tc.get('cant_vote', 0) or 0)
                    ro_need_asst += int(tc.get('need_assistance', 0) or 0)
                if t in taluk_assistance:
                    for item in taluk_assistance[t]:
                        key = item['assistance_type']
                        asst_map[key] = asst_map.get(key, 0) + item['count']

            ro_breakdown.append({
                'ro_name':         ro['ro_name']   or 'NA',
                'ro_phone':        ro['ro_phone']  or 'NA',
                'aro_name':        ro['aro_name']  or 'NA',
                'aro_phone':       ro['aro_phone'] or 'NA',
                'ac_name':         ac_name         or 'NA',
                'total_queries':   ro_total,
                'will_vote':       ro_will_vote,
                'cant_vote':  ro_cant_vote,
                'need_assistance': ro_need_asst,
                'assistance_breakdown': sorted(
                    [{'assistance_type': k, 'count': v} for k, v in asst_map.items()],
                    key=lambda x: -x['count']
                ),
            })

        # Sort by total queries descending
        ro_breakdown.sort(key=lambda x: -x['total_queries'])

        # ── 6. Cumulative = sum of RO rows (guaranteed to match) ──────────────
        cum_total      = sum(r['total_queries']   for r in ro_breakdown)
        cum_will_vote  = sum(r['will_vote']        for r in ro_breakdown)
        cum_cant_vote = sum(r['cant_vote'] for r in ro_breakdown)
        cum_need_asst  = sum(r['need_assistance']  for r in ro_breakdown)

        # Cumulative assistance: merge all RO assistance_breakdown dicts
        cum_asst_map: dict = {}
        for r in ro_breakdown:
            for item in r['assistance_breakdown']:
                k = item['assistance_type']
                cum_asst_map[k] = cum_asst_map.get(k, 0) + item['count']

        assistance_totals = sorted(
            [{'assistance_type': k, 'count': v} for k, v in cum_asst_map.items()],
            key=lambda x: -x['count']
        )

        return {
            "totals": {
                "total_queries":   cum_total,
                "will_vote":       cum_will_vote,
                "cant_vote":  cum_cant_vote,
                "need_assistance": cum_need_asst,
            },
            "assistance_totals": assistance_totals,
            "ro_breakdown":      ro_breakdown,
        }

    except Exception as e:
        return {"error": str(e)}

@app.post("/ro-login")
def ro_login(
    phone: str = Query(...),
    epic:  str = Query(...),
):
    """
    Authenticate RO/ARO.
    Table: ro_datas
    phone = phone column
    epic  = epic column
    """
    try:
        conn = get_connection()
        cur  = conn.cursor()
        cur.execute("""
            SELECT id, assembly_no, assembly_name, officer_type,
                   name, designation, department, phone
            FROM ro_datas
            WHERE TRIM(phone) = %s
              AND TRIM(epic)  = %s
            LIMIT 1;
        """, (phone.strip(), epic.strip()))
        row = cur.fetchone()
        cur.close()
        conn.close()

        if not row:
            return {"success": False, "error": "Invalid phone number or password"}

        return {
            "success":       True,
            "ro_name":       row["name"]           or "NA",
            "ro_phone":      row["phone"]           or phone,
            "designation":   row["designation"]     or "NA",
            "officer_type":  row["officer_type"]    or "NA",
            "department":    row["department"]      or "NA",
            "assembly_name": row["assembly_name"]   or "NA",
            "assembly_no":   str(row["assembly_no"] or "NA"),
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@app.get("/ro-my-queries")
def get_ro_my_queries(assembly_name: str = Query(...)):
    """
    Fetch queries for the logged-in RO by matching
    ro_datas.assembly_name  →  chatbot_voter_logs.taluk  (case-insensitive)
    """
    try:
        conn = get_connection()
        cur  = conn.cursor()

        # Direct match: assembly_name from ro_datas vs taluk in chatbot_voter_logs
        # Also use TALUK_GROUP_MAP to catch aliased taluk names
        matching_taluks = get_matching_taluks(assembly_name)

        if not matching_taluks:
            return {"assembly_name": assembly_name, "count": 0, "data": []}

        placeholders = ','.join(['%s'] * len(matching_taluks))
        cur.execute(f"""
            SELECT
                id,
                voter_name,
                disability_type,
                taluk,
                will_vote,
                need_assistance,
                assistance_type,
                applied_date::text,
                applied_time::text,
                mobile_number,
                COALESCE(current_status, 'pending') AS current_status
            FROM chatbot_voter_logs
            WHERE LOWER(TRIM(taluk)) IN ({placeholders})
              AND (current_status IS NULL OR current_status != 'checked')
            ORDER BY applied_date DESC, applied_time DESC;
        """, [t.lower().strip() for t in matching_taluks])

        rows = cur.fetchall()
        cur.close()
        conn.close()

        data = [{k: (str(v) if v is not None else 'NA')
                 for k, v in row.items()} for row in rows]
        return {"assembly_name": assembly_name, "count": len(data), "data": data}
    except Exception as e:
        return {"error": str(e)}


@app.get("/ro-my-queries/assistance-summary")
def get_ro_my_assistance_summary(assembly_name: str = Query(...)):
    """
    Assistance summary for the logged-in RO's assembly.
    """
    try:
        conn = get_connection()
        cur  = conn.cursor()

        matching_taluks = get_matching_taluks(assembly_name)
        if not matching_taluks:
            return {"assembly_name": assembly_name, "summary": []}

        placeholders = ','.join(['%s'] * len(matching_taluks))
        cur.execute(f"""
            SELECT
                assistance_type,
                COUNT(*) AS count
            FROM chatbot_voter_logs
            WHERE LOWER(TRIM(taluk)) IN ({placeholders})
              AND need_assistance = true
              AND (current_status IS NULL OR current_status != 'checked')
            GROUP BY assistance_type
            ORDER BY count DESC;
        """, [t.lower().strip() for t in matching_taluks])

        rows = cur.fetchall()
        cur.close()
        conn.close()
        return {"assembly_name": assembly_name, "summary": [dict(r) for r in rows]}
    except Exception as e:
        return {"error": str(e)}
