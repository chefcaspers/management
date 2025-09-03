import marimo

__generated_with = "0.15.2"
app = marimo.App()


@app.cell
def _():
    # '%pip install simpy Flask-SQLAlchemy' command supported automatically in marimo
    return


app._unparsable_cell(
    r"""
    import uuid, random, json, sys
    from datetime import datetime, timedelta
    import simpy
    from simpy.rt import RealtimeEnvironment
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker

    # ── quick relative import ────────────────────────────────────────────────────
    sys.path.append(\"../backend\")
    from models import Brand

    # ── CONFIG ───────────────────────────────────────────────────────────────────
    AVG_DAILY_ORDERS = 5                          # your target average
    SIM_LOOKBACK_DAYS = 5                         # how far back to back‑fill

    DB_URL = # FILL ME IN

    # ── TIMING CUTOFFS ───────────────────────────────────────────────────────────
    START_NOW = datetime.utcnow().replace(microsecond=0)       # second‑level cutoff
    SIM_START = START_NOW - timedelta(days=SIM_LOOKBACK_DAYS)

    # ── DB & LOOK‑UPS ────────────────────────────────────────────────────────────
    session = sessionmaker(bind=create_engine(DB_URL))()
    brands = session.query(Brand).all()
    items_by_brand = {b.id: [i for c in b.menu.categories for i in c.items]
                      for b in brands if b.menu}

    # ── HELPERS ──────────────────────────────────────────────────────────────────
    def daily_order_count(day):
        bump  = [0.8, 0.9, 1.0, 1.1, 1.2, 1.5, 1.3][day.weekday()]         # Fri/Sat spike
        return int(AVG_DAILY_ORDERS * bump * random.uniform(0.85, 1.15))

    def generate_order(ts=None):
        brand  = random.choice(brands)
        items  = random.sample(items_by_brand[brand.id], k=random.randint(1, 3))
        return {
            \"order_id\": str(uuid.uuid4()),
            \"timestamp\": (ts or datetime.utcnow()).isoformat(),
            \"customer\": random.choice([\"Alice\", \"Bob\", \"Charlie\", \"Dana\"]),
            \"address\":  \"123 Any St.\",
            \"brand\":    brand.name,
            \"items\":    [{\"id\": i.id, \"name\": i.name, \"price\": i.price} for i in items],
            \"total\":    round(sum(i.price for i in items), 2),
        }

    def send(evt):
        print(json.dumps(evt))

    # ── BACK‑FILL PROCESS (up to START_NOW‑1 s) ──────────────────────────────────
    def backfill(env):
        day = SIM_START.date()
        today = START_NOW.date()

        while day <= today:
            # seconds in the day we are allowed to use
            if day < today:
                end_sec = 86_400                                # full day
            else:
                end_sec = int((START_NOW - datetime.combine(day, datetime.min.time())
                               ).total_seconds())               # up‑to‑second cutoff
            if end_sec == 0:    # Script started at exact midnight; nothing to back‑fill
                break

            # scale order count by fraction of the day available (for cutoff day)
            fraction = end_sec / 86_400
            n_orders = max(1, int(daily_order_count(day) * fraction))

            secs = sorted(random.randrange(end_sec) for _ in range(n_orders))
            for sec in secs:
                ts = datetime.combine(day, datetime.min.time()) + timedelta(seconds=sec)
                send(generate_order(ts))
                yield env.timeout(0)     # keep generator alive

            day += timedelta(days=1)

    # ── REAL‑TIME PROCESS (wall‑clock) ───────────────────────────────────────────
    def realtime(env):
        while True:
            l = AVG_DAILY_ORDERS / 86_400
            yield env.timeout(random.expovariate(l))     # real seconds, thanks to RT env
            send(generate_order())

    # ── EXECUTION ────────────────────────────────────────────────────────────────
    print(\"backfill …\")
    hist_env = simpy.Environment()
    hist_env.process(backfill(hist_env))
    hist_env.run()                                       # completes instantly

    print(\"realtime …\")
    rt_env = RealtimeEnvironment(factor=1)               # sim‑sec == real‑sec
    rt_env.process(realtime(rt_env))
    rt_env.run()                                         # runs indefinitely
    """,
    name="_"
)


if __name__ == "__main__":
    app.run()
