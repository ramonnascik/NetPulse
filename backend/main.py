"""NetPulse - backend (FastAPI + PostgreSQL). Faz ping ICMP real nos equipamentos."""
import asyncio
import ipaddress
import math
import os
import re
import time
from asyncio.subprocess import DEVNULL, PIPE
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from typing import Any

import bcrypt
import jwt
import psycopg
from fastapi import Depends, FastAPI, HTTPException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.staticfiles import StaticFiles
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from pydantic import BaseModel, Field

DB_URL = os.getenv("DATABASE_URL", "postgresql://netpulse:netpulse@db:5432/netpulse")
SECRET = os.getenv("SECRET_KEY", "troque-esta-chave")
INTERVAL = int(os.getenv("POLL_INTERVAL", "5"))      # segundos entre rodadas de ping
TIMEOUT = int(os.getenv("PING_TIMEOUT", "2"))        # timeout de cada ping (s)
SLOW_MS = float(os.getenv("SLOW_MS", "100"))         # acima disso = "Lento"
FAILS = int(os.getenv("FAIL_THRESHOLD", "2"))        # falhas seguidas para marcar Offline
RETENTION_DAYS = int(os.getenv("RETENTION_DAYS", "7"))
TYPES = {"router", "switch", "server", "ap", "camera", "pc", "dvr", "retificadora", "olt", "regua", "totem"}

pool = ConnectionPool(DB_URL, min_size=1, max_size=10, kwargs={"row_factory": dict_row}, open=False)
STATE: dict = {}   # estado ao vivo por equipamento
UPT: dict = {}     # (verificações, sucessos) nas últimas 24h
LOGIN_FAIL: dict = {}

SCHEMA = """
CREATE TABLE IF NOT EXISTS users(
  id SERIAL PRIMARY KEY, username TEXT UNIQUE NOT NULL, pw_hash TEXT NOT NULL,
  role TEXT NOT NULL DEFAULT 'viewer', created_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE IF NOT EXISTS devices(
  id SERIAL PRIMARY KEY, name TEXT NOT NULL, ip TEXT UNIQUE NOT NULL,
  type TEXT NOT NULL DEFAULT 'server', parent_id INT REFERENCES devices(id) ON DELETE SET NULL,
  x REAL DEFAULT 0, y REAL DEFAULT 0, created_at TIMESTAMPTZ DEFAULT now());
CREATE TABLE IF NOT EXISTS checks(
  device_id INT REFERENCES devices(id) ON DELETE CASCADE, ts TIMESTAMPTZ DEFAULT now(),
  ok BOOLEAN NOT NULL, latency REAL);
CREATE INDEX IF NOT EXISTS checks_dev_ts ON checks(device_id, ts DESC);
CREATE TABLE IF NOT EXISTS events(
  id SERIAL PRIMARY KEY, device_id INT, name TEXT, status TEXT, ts TIMESTAMPTZ DEFAULT now());
"""


MIGRATE = """
CREATE TABLE IF NOT EXISTS maps(id SERIAL PRIMARY KEY, name TEXT NOT NULL, created_at TIMESTAMPTZ DEFAULT now());
INSERT INTO maps(name) SELECT 'Mapa principal' WHERE NOT EXISTS (SELECT 1 FROM maps);
ALTER TABLE devices ADD COLUMN IF NOT EXISTS map_id INT REFERENCES maps(id) ON DELETE CASCADE;
UPDATE devices SET map_id=(SELECT min(id) FROM maps) WHERE map_id IS NULL;
ALTER TABLE devices DROP CONSTRAINT IF EXISTS devices_ip_key;
CREATE UNIQUE INDEX IF NOT EXISTS devices_map_ip ON devices(map_id, ip);
"""


def q(sql, args=None, fetch="all"):
    with pool.connection() as c:
        cur = c.execute(sql, args)
        if fetch == "all":
            return cur.fetchall()
        if fetch == "one":
            return cur.fetchone()


# ---------------------------------------------------------------- ping real
PING_RE = re.compile(r"time[=<]([\d.]+)")


async def ping(ip: str):
    p = None
    try:
        p = await asyncio.create_subprocess_exec(
            "ping", "-c", "1", "-W", str(TIMEOUT), ip, stdout=PIPE, stderr=DEVNULL)
        out, _ = await asyncio.wait_for(p.communicate(), TIMEOUT + 2)
        m = PING_RE.search(out.decode(errors="ignore"))
        return float(m.group(1)) if p.returncode == 0 and m else None
    except Exception:
        if p and p.returncode is None:
            p.kill()
        return None


def persist(rows, events, clean):
    with pool.connection() as c:
        with c.cursor() as cur:
            if rows:
                cur.executemany("INSERT INTO checks(device_id,ok,latency) VALUES(%s,%s,%s)", rows)
            if events:
                cur.executemany("INSERT INTO events(device_id,name,status) VALUES(%s,%s,%s)", events)
            if clean:
                cur.execute("DELETE FROM checks WHERE ts < now() - make_interval(days => %s)", (RETENTION_DAYS,))
                cur.execute("DELETE FROM events WHERE ts < now() - interval '30 days'")
            cur.execute("""SELECT device_id, count(*) n, count(*) FILTER (WHERE ok) u FROM checks
                           WHERE ts > now() - interval '24 hours' GROUP BY device_id""")
            data = {r["device_id"]: (r["n"], r["u"]) for r in cur.fetchall()}
    UPT.clear()
    UPT.update(data)


async def poller():
    sem = asyncio.Semaphore(50)
    last_clean = 0.0
    while True:
        t0 = time.monotonic()
        try:
            devs = await asyncio.to_thread(q, "SELECT id,name,ip,parent_id FROM devices")

            async def one(d):
                async with sem:
                    return d["id"], await ping(d["ip"])

            res = dict(await asyncio.gather(*[one(d) for d in devs]))
            by = {d["id"]: d for d in devs}
            base, rows = {}, []
            for d in devs:
                s = STATE.setdefault(d["id"], {"st": "pending", "lat": None, "fails": 0, "hist": deque(maxlen=40)})
                lat = res[d["id"]]
                rows.append((d["id"], lat is not None, lat))
                s["hist"].append(lat)
                s["lat"] = lat
                if lat is not None:
                    s["fails"] = 0
                    base[d["id"]] = "warn" if lat > SLOW_MS else "up"
                else:
                    s["fails"] += 1
                    keep = s["st"] in ("up", "warn") and s["fails"] < FAILS  # tolera perda isolada
                    base[d["id"]] = s["st"] if keep else "down"

            def upstream_down(i):
                seen, p = set(), by[i]["parent_id"]
                while p and p in base and p not in seen:
                    if base[p] == "down":
                        return True
                    seen.add(p)
                    p = by[p]["parent_id"]
                return False

            events = []
            for d in devs:
                i = d["id"]
                new = "unreach" if base[i] == "down" and upstream_down(i) else base[i]
                old = STATE[i]["st"]
                STATE[i]["st"] = new
                if old != "pending" and old != new and {old, new} != {"up", "warn"}:
                    events.append((i, d["name"], new))
            clean = time.time() - last_clean > 3600
            await asyncio.to_thread(persist, rows, events, clean)
            if clean:
                last_clean = time.time()
        except Exception as e:  # nunca derruba o monitoramento
            print("poller error:", e, flush=True)
        await asyncio.sleep(max(0.5, INTERVAL - (time.monotonic() - t0)))


# ---------------------------------------------------------------- app / auth
@asynccontextmanager
async def lifespan(app):
    pool.open(wait=True, timeout=60)
    q(SCHEMA, None, None)
    q(MIGRATE, None, None)
    if not q("SELECT 1 FROM users LIMIT 1", None, "one"):
        q("INSERT INTO users(username,pw_hash,role) VALUES(%s,%s,'admin')",
          (os.getenv("ADMIN_USER", "admin"), hash_pw(os.getenv("ADMIN_PASSWORD", "admin123"))), None)
    task = asyncio.create_task(poller())
    yield
    task.cancel()
    pool.close()


app = FastAPI(title="NetPulse", lifespan=lifespan)
bearer = HTTPBearer(auto_error=False)


def hash_pw(p: str) -> str:
    return bcrypt.hashpw(p.encode(), bcrypt.gensalt()).decode()


def check_pw(p: str, h: str) -> bool:
    try:
        return bcrypt.checkpw(p.encode()[:72], h.encode())
    except ValueError:
        return False


def user(cred: HTTPAuthorizationCredentials = Depends(bearer)):
    if not cred:
        raise HTTPException(401, "Não autenticado")
    try:
        uid = int(jwt.decode(cred.credentials, SECRET, algorithms=["HS256"])["sub"])
    except Exception:
        raise HTTPException(401, "Sessão expirada")
    u = q("SELECT id,username,role FROM users WHERE id=%s", (uid,), "one")
    if not u:
        raise HTTPException(401, "Usuário inexistente")
    return u


def admin(u=Depends(user)):
    if u["role"] != "admin":
        raise HTTPException(403, "Apenas administradores")
    return u


class Login(BaseModel):
    username: str
    password: str


class NewUser(BaseModel):
    username: str = Field(min_length=3, max_length=32, pattern=r"^[A-Za-z0-9_.-]+$")
    password: str = Field(min_length=6, max_length=72)
    role: str = Field("viewer", pattern="^(admin|viewer)$")


class Pw(BaseModel):
    password: str = Field(min_length=6, max_length=72)


class DevIn(BaseModel):
    name: str = Field(min_length=1, max_length=40)
    ip: str
    type: str = "server"
    parent: int = 0
    map_id: int
    x: float = 0
    y: float = 0


class DevUp(BaseModel):
    name: str | None = Field(None, min_length=1, max_length=40)
    ip: str | None = None
    type: str | None = None
    parent: int | None = None
    x: float | None = None
    y: float | None = None


def vip(ip: str) -> str:
    try:
        return str(ipaddress.IPv4Address(ip.strip()))
    except Exception:
        raise HTTPException(400, "IP inválido. Use o formato 192.168.88.50")


@app.get("/api/health")
def health():
    return {"ok": True}


@app.post("/api/login")
def login(b: Login):
    k = b.username.lower()
    n, t = LOGIN_FAIL.get(k, (0, 0.0))
    if n >= 8 and time.time() - t < 300:
        raise HTTPException(429, "Muitas tentativas. Aguarde 5 minutos.")
    u = q("SELECT * FROM users WHERE lower(username)=lower(%s)", (b.username,), "one")
    if not u or not check_pw(b.password, u["pw_hash"]):
        LOGIN_FAIL[k] = (n + 1 if time.time() - t < 300 else 1, time.time())
        raise HTTPException(401, "Usuário ou senha incorretos")
    LOGIN_FAIL.pop(k, None)
    exp = datetime.now(timezone.utc) + timedelta(hours=12)
    token = jwt.encode({"sub": str(u["id"]), "exp": exp}, SECRET, algorithm="HS256")
    return {"token": token, "user": {"id": u["id"], "username": u["username"], "role": u["role"]}}


@app.get("/api/me")
def me(u=Depends(user)):
    return u


@app.put("/api/me/password")
def my_pw(b: Pw, u=Depends(user)):
    q("UPDATE users SET pw_hash=%s WHERE id=%s", (hash_pw(b.password), u["id"]), None)
    return {"ok": True}


# ---------------------------------------------------------------- usuários
@app.get("/api/users")
def users(_=Depends(admin)):
    return q("SELECT id,username,role FROM users ORDER BY username")


@app.post("/api/users")
def add_user(b: NewUser, _=Depends(admin)):
    try:
        q("INSERT INTO users(username,pw_hash,role) VALUES(%s,%s,%s)", (b.username, hash_pw(b.password), b.role), None)
    except psycopg.errors.UniqueViolation:
        raise HTTPException(400, "Esse usuário já existe")
    return {"ok": True}


@app.put("/api/users/{uid}/password")
def set_pw(uid: int, b: Pw, _=Depends(admin)):
    q("UPDATE users SET pw_hash=%s WHERE id=%s", (hash_pw(b.password), uid), None)
    return {"ok": True}


@app.delete("/api/users/{uid}")
def del_user(uid: int, u=Depends(admin)):
    if uid == u["id"]:
        raise HTTPException(400, "Você não pode excluir o próprio usuário")
    q("DELETE FROM users WHERE id=%s", (uid,), None)
    return {"ok": True}


# ---------------------------------------------------------------- equipamentos
def first_map():
    return q("SELECT min(id) AS id FROM maps", None, "one")["id"]


class MapIn(BaseModel):
    name: str = Field(min_length=1, max_length=40)


@app.get("/api/maps")
def list_maps(_=Depends(user)):
    by_map = {}
    for d in q("SELECT id,map_id FROM devices"):
        by_map.setdefault(d["map_id"], []).append(d["id"])
    out = []
    for m in q("SELECT id,name FROM maps ORDER BY id"):
        sts = [STATE.get(i, {}).get("st", "pending") for i in by_map.get(m["id"], [])]
        out.append({"id": m["id"], "name": m["name"], "total": len(sts), "up": sts.count("up"),
                    "warn": sts.count("warn"), "down": sts.count("down") + sts.count("unreach")})
    return out


@app.post("/api/maps")
def add_map(m: MapIn, _=Depends(admin)):
    r = q("INSERT INTO maps(name) VALUES(%s) RETURNING id", (m.name.strip(),), "one")
    return {"id": r["id"]}


@app.put("/api/maps/{mid}")
def ren_map(mid: int, m: MapIn, _=Depends(admin)):
    q("UPDATE maps SET name=%s WHERE id=%s", (m.name.strip(), mid), None)
    return {"ok": True}


@app.delete("/api/maps/{mid}")
def del_map(mid: int, _=Depends(admin)):
    if q("SELECT count(*) AS n FROM maps", None, "one")["n"] <= 1:
        raise HTTPException(400, "Mantenha ao menos um mapa")
    for d in q("SELECT id FROM devices WHERE map_id=%s", (mid,)):
        STATE.pop(d["id"], None)
    q("DELETE FROM maps WHERE id=%s", (mid,), None)  # apaga também os equipamentos do mapa
    return {"ok": True}


@app.get("/api/state")
def state(map_id: int | None = None, _=Depends(user)):
    mid = map_id or first_map()
    out = []
    for d in q("SELECT id,name,ip,type,parent_id,x,y FROM devices WHERE map_id=%s ORDER BY id", (mid,)):
        s = STATE.get(d["id"], {})
        n, u = UPT.get(d["id"], (0, 0))
        out.append({"id": d["id"], "name": d["name"], "ip": d["ip"], "type": d["type"],
                    "parent": d["parent_id"] or 0, "x": d["x"], "y": d["y"],
                    "st": s.get("st", "pending"), "lat": s.get("lat"),
                    "hist": list(s.get("hist", [])), "n": n, "u": u})
    ev = q("""SELECT name,status,ts FROM events
              WHERE device_id IN (SELECT id FROM devices WHERE map_id=%s) ORDER BY id DESC LIMIT 8""", (mid,))
    return {"devices": out, "events": ev}


@app.post("/api/devices")
def add_dev(d: DevIn, _=Depends(admin)):
    if d.type not in TYPES:
        raise HTTPException(400, "Tipo inválido")
    if d.parent and not q("SELECT 1 FROM devices WHERE id=%s AND map_id=%s", (d.parent, d.map_id), "one"):
        raise HTTPException(400, "O equipamento pai precisa estar no mesmo mapa")
    try:
        r = q("INSERT INTO devices(name,ip,type,parent_id,x,y,map_id) VALUES(%s,%s,%s,%s,%s,%s,%s) RETURNING id",
              (d.name.strip(), vip(d.ip), d.type, d.parent or None, d.x, d.y, d.map_id), "one")
    except psycopg.errors.UniqueViolation:
        raise HTTPException(400, "Esse IP já está cadastrado neste mapa")
    except psycopg.errors.ForeignKeyViolation:
        raise HTTPException(400, "Mapa não existe")
    return {"id": r["id"]}


@app.put("/api/devices/{did}")
def upd_dev(did: int, d: DevUp, _=Depends(admin)):
    f = {}
    if d.name is not None:
        f["name"] = d.name.strip()
    if d.ip is not None:
        f["ip"] = vip(d.ip)
    if d.type is not None:
        if d.type not in TYPES:
            raise HTTPException(400, "Tipo inválido")
        f["type"] = d.type
    if d.parent is not None:
        if d.parent == did:
            raise HTTPException(400, "Um equipamento não pode ser pai dele mesmo")
        if d.parent and not q("SELECT 1 FROM devices p JOIN devices c ON c.map_id=p.map_id WHERE p.id=%s AND c.id=%s",
                              (d.parent, did), "one"):
            raise HTTPException(400, "O equipamento pai precisa estar no mesmo mapa")
        p, seen = d.parent, set()
        while p and p not in seen:  # impede ciclos (A ligado em B, B ligado em A)
            if p == did:
                raise HTTPException(400, "Isso criaria um ciclo entre equipamentos")
            seen.add(p)
            r = q("SELECT parent_id FROM devices WHERE id=%s", (p,), "one")
            p = r["parent_id"] if r else None
        f["parent_id"] = d.parent or None
    if d.x is not None:
        f["x"] = d.x
    if d.y is not None:
        f["y"] = d.y
    if f:
        try:
            q(f"UPDATE devices SET {','.join(k + '=%s' for k in f)} WHERE id=%s", (*f.values(), did), None)
        except psycopg.errors.UniqueViolation:
            raise HTTPException(400, "Esse IP já está cadastrado neste mapa")
    if "ip" in f:  # IP novo = histórico novo
        STATE.pop(did, None)
        q("DELETE FROM checks WHERE device_id=%s", (did,), None)
    return {"ok": True}


class ImportIn(BaseModel):
    maps: list[dict] | None = None
    devices: list[Any] | None = None
    map_id: int | None = None


def norm_ip(v):
    try:
        return str(ipaddress.IPv4Address(str(v).strip()))
    except Exception:
        return None


def num(v):
    try:
        return float(v) if v is not None and v != "" else None
    except (TypeError, ValueError):
        return None


@app.get("/api/export")
def export_all(_=Depends(admin)):
    out = {"version": 1, "maps": []}
    for m in q("SELECT id,name FROM maps ORDER BY id"):
        rows = q("""SELECT d.name,d.ip,d.type,d.x,d.y,p.ip AS parent FROM devices d
                    LEFT JOIN devices p ON p.id=d.parent_id WHERE d.map_id=%s ORDER BY d.id""", (m["id"],))
        out["maps"].append({"name": m["name"], "devices": [
            {"name": r["name"], "ip": r["ip"], "type": r["type"], "parent": r["parent"],
             "x": round(r["x"]), "y": round(r["y"])} for r in rows]})
    return out


@app.post("/api/import")
def import_json(b: ImportIn, _=Depends(admin)):
    if b.maps is not None:
        groups = [(str(m.get("name") or "").strip()[:40] or "Mapa importado", m.get("devices") or []) for m in b.maps]
    elif b.devices is not None:
        groups = [(None, b.devices)]
    else:
        raise HTTPException(400, 'O JSON precisa ter "maps" ou "devices"')
    if sum(len(g[1]) for g in groups) > 2000:
        raise HTTPException(400, "Limite de 2000 equipamentos por importação")
    res = {"maps_created": 0, "added": 0, "skipped": 0, "errors": []}
    err = res["errors"].append
    with pool.connection() as c:
        for mname, items in groups:
            if mname is None:
                mid = b.map_id or first_map()
            else:
                m = c.execute("SELECT id FROM maps WHERE lower(name)=lower(%s) ORDER BY id LIMIT 1", (mname,)).fetchone()
                if m:
                    mid = m["id"]
                else:
                    mid = c.execute("INSERT INTO maps(name) VALUES(%s) RETURNING id", (mname,)).fetchone()["id"]
                    res["maps_created"] += 1
            label = mname or "mapa atual"
            new = []
            for i, it in enumerate(items, 1):
                if not isinstance(it, dict):
                    err(f"{label} #{i}: item inválido")
                    continue
                name, ip = str(it.get("name") or "").strip()[:40], norm_ip(it.get("ip"))
                typ = str(it.get("type") or "server").strip().lower()
                if not name or not ip:
                    err(f"{label} #{i}: nome ou IP inválido")
                    continue
                if typ not in TYPES:
                    err(f"{label} ({name}): tipo '{typ}' desconhecido, usado 'server'")
                    typ = "server"
                x, y = num(it.get("x")), num(it.get("y"))
                if x is None or y is None:
                    x = y = None
                row = c.execute("""INSERT INTO devices(name,ip,type,x,y,map_id) VALUES(%s,%s,%s,%s,%s,%s)
                                   ON CONFLICT (map_id, ip) DO NOTHING RETURNING id""",
                                (name, ip, typ, x or 0, y or 0, mid)).fetchone()
                if not row:
                    res["skipped"] += 1
                    continue
                res["added"] += 1
                new.append({"id": row["id"], "name": name, "x": x, "y": y, "praw": it.get("parent"), "pid": None})
            rows = c.execute("SELECT id,ip,parent_id,x,y FROM devices WHERE map_id=%s", (mid,)).fetchall()
            by_ip = {r["ip"]: r["id"] for r in rows}
            par = {r["id"]: r["parent_id"] for r in rows}
            for n in new:  # liga cada equipamento ao pai (pelo IP)
                if not n["praw"]:
                    continue
                pid = by_ip.get(norm_ip(n["praw"]))
                if not pid or pid == n["id"]:
                    err(f"{label} ({n['name']}): pai '{n['praw']}' não encontrado neste mapa")
                    continue
                p, seen, cyc = pid, set(), False
                while p and p not in seen:
                    if p == n["id"]:
                        cyc = True
                        break
                    seen.add(p)
                    p = par.get(p)
                if cyc:
                    err(f"{label} ({n['name']}): ligação com '{n['praw']}' criaria um ciclo")
                    continue
                par[n["id"]] = n["pid"] = pid
                c.execute("UPDATE devices SET parent_id=%s WHERE id=%s", (pid, n["id"]))
            todo = [n for n in new if n["x"] is None]  # sem posição: organiza automaticamente
            skip = {n["id"] for n in todo}
            pos = {r["id"]: (r["x"], r["y"]) for r in rows if r["id"] not in skip}
            cnt = {}

            def put(n, xy):
                pos[n["id"]] = xy
                c.execute("UPDATE devices SET x=%s,y=%s WHERE id=%s", (xy[0], xy[1], n["id"]))

            for _ in range(12):
                left = []
                for n in todo:
                    if n["pid"] and n["pid"] in pos:
                        k = cnt.get(n["pid"], 0)
                        cnt[n["pid"]] = k + 1
                        ang, rad = -math.pi / 2 + k * 0.9, 190 + 45 * (k // 7)
                        px, py = pos[n["pid"]]
                        put(n, (round(px + math.cos(ang) * rad), round(py + math.sin(ang) * rad)))
                    else:
                        left.append(n)
                if len(left) == len(todo):
                    break
                todo = left
            base_y = (max(p[1] for p in pos.values()) + 200) if pos else 120
            for i, n in enumerate(todo):
                put(n, (120 + (i % 5) * 200, base_y + (i // 5) * 170))
    res["error_count"] = len(res["errors"])
    res["errors"] = res["errors"][:30]
    return res


@app.delete("/api/devices/{did}")
def del_dev(did: int, _=Depends(admin)):
    q("DELETE FROM devices WHERE id=%s", (did,), None)
    STATE.pop(did, None)
    return {"ok": True}


app.mount("/", StaticFiles(directory="static", html=True), name="static")
