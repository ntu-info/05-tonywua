# app.py
from flask import Flask, jsonify, abort, send_file
import os
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.exc import OperationalError

_engine = None

def get_engine():
    global _engine
    if _engine is not None:
        return _engine
    db_url = os.getenv("DB_URL")
    if not db_url:
        raise RuntimeError("Missing DB_URL (or DATABASE_URL) environment variable.")
    # Normalize old 'postgres://' scheme to 'postgresql://'
    if db_url.startswith("postgres://"):
        db_url = "postgresql://" + db_url[len("postgres://"):]
    _engine = create_engine(
        db_url,
        pool_pre_ping=True,
    )
    return _engine

def create_app():
    app = Flask(__name__)

    @app.get("/", endpoint="health")
    def health():
        return "<p>Server working!</p>"

    @app.get("/img", endpoint="show_img")
    def show_img():
        return send_file("amygdala.gif", mimetype="image/gif")

    # ① GET /terms/<term>/studies
    @app.get("/terms/<term>/studies", endpoint="terms_studies")
    def get_studies_by_term(term):
        try:
            eng = get_engine()
            with eng.connect() as conn:
                conn.execute(text("SET search_path TO ns, public;"))
                rows = conn.execute(text("""
                    SELECT DISTINCT study_id
                    FROM ns.annotations_terms
                    WHERE term = :t
                    ORDER BY study_id
                """), {"t": term}).mappings().all()
            return jsonify([r["study_id"] for r in rows])
        except Exception:
            return jsonify({"error": "db query failed"}), 500

    # ② GET /locations/<x_y_z>/studies
    @app.get("/locations/<coords>/studies", endpoint="locations_studies")
    def get_studies_by_coordinates(coords):
        try:
            parts = coords.split("_")
            if len(parts) != 3:
                abort(400, "coords must be 'x_y_z', e.g., 0_-52_26")
            x, y, z = map(float, parts)
        except Exception:
            abort(400, "coords must be 'x_y_z', e.g., 0_-52_26")
        try:
            eng = get_engine()
            with eng.connect() as conn:
                conn.execute(text("SET search_path TO ns, public;"))
                rows = conn.execute(text("""
                    SELECT DISTINCT study_id
                    FROM ns.coordinates
                    WHERE ST_X(geom) = :x AND ST_Y(geom) = :y AND ST_Z(geom) = :z
                    ORDER BY study_id
                """), {"x": x, "y": y, "z": z}).mappings().all()
            return jsonify([r["study_id"] for r in rows])
        except Exception:
            return jsonify({"error": "db query failed"}), 500

    # ③ GET /dissociate/terms/<term_a>/<term_b>
    @app.get("/dissociate/terms/<term_a>/<term_b>", endpoint="dissociate_terms")
    def dissociate_by_terms(term_a, term_b):
        try:
            eng = get_engine()
            with eng.connect() as conn:
                conn.execute(text("SET search_path TO ns, public;"))
                rows = conn.execute(text("""
                    SELECT DISTINCT a.study_id, m.title
                    FROM ns.annotations_terms a
                    JOIN ns.metadata m ON m.study_id = a.study_id
                    WHERE a.term = :ta
                      AND NOT EXISTS (
                          SELECT 1 FROM ns.annotations_terms b
                          WHERE b.study_id = a.study_id AND b.term = :tb
                      )
                    ORDER BY a.study_id
                """), {"ta": term_a, "tb": term_b}).mappings().all()
            return jsonify([
                {"study_id": r["study_id"], "title": r["title"]}
                for r in rows
            ])
        except Exception:
            return jsonify({"error": "db query failed"}), 500

    # ④ GET /dissociate/locations/<x1_y1_z1>/<x2_y2_z2>
    @app.get("/dissociate/locations/<c1>/<c2>", endpoint="dissociate_locations")
    def dissociate_by_locations(c1, c2):
        try:
            parts1 = c1.split("_")
            parts2 = c2.split("_")
            if len(parts1) != 3 or len(parts2) != 3:
                abort(400, "coords must be 'x_y_z', e.g., 0_-52_26")
            x1, y1, z1 = map(float, parts1)
            x2, y2, z2 = map(float, parts2)
        except Exception:
            abort(400, "coords must be 'x_y_z', e.g., 0_-52_26")
        try:
            eng = get_engine()
            with eng.connect() as conn:
                conn.execute(text("SET search_path TO ns, public;"))
                rows = conn.execute(text("""
                    SELECT DISTINCT a.study_id, m.title
                    FROM ns.coordinates a
                    JOIN ns.metadata m ON m.study_id = a.study_id
                    WHERE ST_X(a.geom) = :x1 AND ST_Y(a.geom) = :y1 AND ST_Z(a.geom) = :z1
                      AND NOT EXISTS (
                          SELECT 1 FROM ns.coordinates b
                          WHERE b.study_id = a.study_id
                            AND ST_X(b.geom) = :x2 AND ST_Y(b.geom) = :y2 AND ST_Z(b.geom) = :z2
                      )
                    ORDER BY a.study_id
                """), {
                    "x1": x1, "y1": y1, "z1": z1,
                    "x2": x2, "y2": y2, "z2": z2
                }).mappings().all()
            coord = [x1, y1, z1]
            return jsonify([
                {"coord": coord, "study_id": r["study_id"], "title": r["title"]}
                for r in rows
            ])
        except Exception:
            return jsonify({"error": "db query failed"}), 500

    @app.get("/test_db", endpoint="test_db")
    def test_db():
        eng = get_engine()
        payload = {"ok": False, "dialect": eng.dialect.name}

        try:
            with eng.begin() as conn:
                # Ensure we are in the correct schema
                conn.execute(text("SET search_path TO ns, public;"))
                payload["version"] = conn.exec_driver_sql("SELECT version()").scalar()

                # Counts
                payload["coordinates_count"] = conn.execute(text("SELECT COUNT(*) FROM ns.coordinates")).scalar()
                payload["metadata_count"] = conn.execute(text("SELECT COUNT(*) FROM ns.metadata")).scalar()
                payload["annotations_terms_count"] = conn.execute(text("SELECT COUNT(*) FROM ns.annotations_terms")).scalar()

                # Samples
                try:
                    rows = conn.execute(text(
                        "SELECT study_id, ST_X(geom) AS x, ST_Y(geom) AS y, ST_Z(geom) AS z FROM ns.coordinates LIMIT 3"
                    )).mappings().all()
                    payload["coordinates_sample"] = [dict(r) for r in rows]
                except Exception:
                    payload["coordinates_sample"] = []

                try:
                    rows = conn.execute(text("SELECT * FROM ns.metadata LIMIT 3")).mappings().all()
                    payload["metadata_sample"] = [dict(r) for r in rows]
                except Exception:
                    payload["metadata_sample"] = []

                try:
                    rows = conn.execute(text(
                        "SELECT study_id, contrast_id, term, score FROM ns.annotations_terms LIMIT 20"
                    )).mappings().all()
                    payload["annotations_terms_sample"] = [dict(r) for r in rows]
                except Exception:
                    payload["annotations_terms_sample"] = []

            payload["ok"] = True
            return jsonify(payload), 200

        except Exception:
            payload["error"] = "db query failed"
            return jsonify(payload), 500

    return app

# WSGI entry point (no __main__)
app = create_app()