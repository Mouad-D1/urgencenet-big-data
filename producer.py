#!/usr/bin/env python3
"""Generateur d'evenements de passages aux urgences (donnees entierement synthetiques).

    python producer.py --mode file --n 50000     # historique data/urgences_history.jsonl
    python producer.py --mode kafka --rate 20    # flux continu vers le topic urgences

Simule volontairement ~5 % d'evenements en retard (jusqu'a 15 minutes) et ~2 % de doublons
(meme visit_id et meme status). Un retard decale l'emission, jamais event_time : l'ecart entre
ARRIVED et CARE_STARTED d'un passage reste donc toujours egal a wait_minutes.

En mode file, des cas inhabituels sont injectes pour la Partie 6 et decrits dans
data/anomalies_ground_truth.json, ce qui permet d'evaluer les regles d'alerte.
"""
import argparse
import heapq
import json
import os
import random
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent

LATE_RATE = 0.05
LATE_MIN_SECONDS, LATE_MAX_SECONDS = 60, 900
DUPLICATE_RATE = 0.02

PRIORITY_WEIGHTS = {1: 5, 2: 15, 3: 35, 4: 30, 5: 15}
# (min, mode, max) du delai d'attente en minutes, par priorite
WAIT_MINUTES = {1: (0, 3, 10), 2: (5, 12, 25), 3: (10, 25, 50), 4: (20, 45, 80), 5: (30, 60, 120)}

DEPARTMENTS = {
    "CHU": ["Traumatologie", "Cardiologie", "Pédiatrie", "Neurologie", "Médecine générale", "Gynécologie", "Pneumologie"],
    "HOPITAL_REGIONAL": ["Traumatologie", "Cardiologie", "Pédiatrie", "Médecine générale", "Gynécologie"],
    "CLINIQUE": ["Traumatologie", "Pédiatrie", "Médecine générale", "Gynécologie"],
    "CENTRE_SANTE": ["Pédiatrie", "Médecine générale"],
}
DESKS_PER_TYPE = {"CHU": 4, "HOPITAL_REGIONAL": 3, "CLINIQUE": 2, "CENTRE_SANTE": 2}
CITY_CODES = {"Casablanca": "CAS", "Rabat": "RAB", "Marrakech": "MAR", "Tanger": "TNG"}

TRANSFER_RATE = {"CHU": 0.03, "HOPITAL_REGIONAL": 0.08, "CLINIQUE": 0.10, "CENTRE_SANTE": 0.15}
CROSS_GROUP_SHARE = 0.05  # part des transferts vers un CHU d'un autre groupe (seul cas autorise)
FOLLOW_ON_RATE = 0.7      # part des patients transferes dont l'arrivee a destination est observee
MAX_CHAIN_DEPTH = 3

# Chaque poste alterne activite et pause : sans ces pauses, les fenetres de session
# (5 minutes d'inactivite, Partie 5) ne se fermeraient jamais. Le decalage entre postes
# d'un meme etablissement garantit qu'au moins un poste reste actif.
DESK_PERIOD_SECONDS = 20 * 60
DESK_ACTIVE_SECONDS = 12 * 60

PATIENT_POOL = 10_000
MAX_VISIT_SECONDS = 4 * 3600
AVERAGE_EVENTS_PER_VISIT = 4 * (1 + DUPLICATE_RATE) * 1.05

# Nombre de scenarios injectes pour n = 50 000 (proportionnel a n)
ANOMALY_BASE_COUNTS = {
    "frequent_transfers": 6,
    "transfer_loop": 8,
    "cross_group_non_chu": 10,
    "receiver_hub": 60,
    "sender_hub": 60,
}


def format_time(ts):
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_facilities(path):
    with open(path, encoding="utf-8") as f:
        return [json.loads(line) for line in f if line.strip()]


class Simulator:
    def __init__(self, facilities, rng, first_visit_id=1):
        self.rng = rng
        self.facilities = facilities
        self.next_visit_id = first_visit_id
        self.arrival_weights = [f["beds"] ** 0.5 for f in facilities]
        self.by_group = defaultdict(list)
        for f in facilities:
            self.by_group[f["hospital_group"]].append(f)
        self.desks = self._build_desks()
        self.patients = self._build_patient_pools()

    def _build_desks(self):
        # Numerotation par ville, conforme au format T-CAS-03
        counters = Counter()
        desks = {}
        for f in self.facilities:
            count = DESKS_PER_TYPE[f["type"]]
            desks[f["facility_id"]] = []
            for i in range(count):
                counters[f["city"]] += 1
                desk_id = f"T-{CITY_CODES[f['city']]}-{counters[f['city']]:02d}"
                desks[f["facility_id"]].append((desk_id, i * DESK_PERIOD_SECONDS // count))
        return desks

    def _build_patient_pools(self):
        # Un patient est rattache a une ville : ses admissions restent proches de chez lui
        weights = Counter()
        for f, w in zip(self.facilities, self.arrival_weights):
            weights[f["city"]] += w
        total = sum(weights.values())
        pools, start = {}, 1
        for city, w in sorted(weights.items()):
            size = int(PATIENT_POOL * w / total)
            pools[city] = range(start, start + size)
            start += size
        return pools

    def pick_facility(self):
        return self.rng.choices(self.facilities, weights=self.arrival_weights)[0]

    def pick_patient(self, city):
        return f"P-{self.rng.choice(self.patients[city]):05d}"

    def pick_desk(self, facility, ts):
        desks = self.desks[facility["facility_id"]]
        active = [d for d, phase in desks if (ts + phase) % DESK_PERIOD_SECONDS < DESK_ACTIVE_SECONDS]
        return self.rng.choice(active or [desks[0][0]])

    def pick_transfer_destination(self, facility):
        group = facility["hospital_group"]
        if self.rng.random() < CROSS_GROUP_SHARE:
            candidates = [f for f in self.facilities if f["hospital_group"] != group and f["type"] == "CHU"]
        else:
            candidates = [f for f in self.by_group[group] if f["facility_id"] != facility["facility_id"]]
        return self.rng.choices(candidates, weights=[f["beds"] for f in candidates])[0]

    def visit_events(self, arrival, facility, patient_id, department=None, transfer_to=None):
        """Retourne les evenements d'un passage complet et l'horodatage de sa fin."""
        rng = self.rng
        visit_id = f"V-{self.next_visit_id:07d}"
        self.next_visit_id += 1

        departments = DEPARTMENTS[facility["type"]]
        if department not in departments:
            department = rng.choice(departments)
        priority = rng.choices(list(PRIORITY_WEIGHTS), weights=PRIORITY_WEIGHTS.values())[0]
        low, mode, high = WAIT_MINUTES[priority]
        wait_minutes = round(rng.triangular(low, high, mode))

        arrival = int(arrival)
        care = arrival + wait_minutes * 60
        triage = arrival + rng.randint(0, min(10, wait_minutes) * 60)
        end = care + rng.randint(10, MAX_VISIT_SECONDS // 60 - 120) * 60

        def event(status, ts, **fields):
            return {
                "visit_id": visit_id,
                "patient_id": patient_id,
                "facility_id": facility["facility_id"],
                "city": facility["city"],
                "status": status,
                "priority": fields.get("priority"),
                "wait_minutes": fields.get("wait_minutes"),
                "department": department,
                "desk_id": fields.get("desk_id"),
                "transfer_to": fields.get("transfer_to"),
                "event_time": format_time(ts),
            }, ts

        events = [
            event("ARRIVED", arrival),
            event("TRIAGED", triage, priority=priority, desk_id=self.pick_desk(facility, triage)),
            event("CARE_STARTED", care, priority=priority, wait_minutes=wait_minutes),
        ]
        if transfer_to:
            events.append(event("TRANSFERRED", end, priority=priority, transfer_to=transfer_to["facility_id"]))
        else:
            events.append(event("DISCHARGED", end, priority=priority))
        return events, end, department, visit_id

    def patient_journey(self, arrival, facility, patient_id=None, depth=0):
        """Passage standard, suivi le cas echeant du passage dans l'etablissement de destination."""
        patient_id = patient_id or self.pick_patient(facility["city"])
        transfer_to = None
        if self.rng.random() < TRANSFER_RATE[facility["type"]]:
            transfer_to = self.pick_transfer_destination(facility)
        events, end, department, _ = self.visit_events(arrival, facility, patient_id, transfer_to=transfer_to)
        if transfer_to and depth < MAX_CHAIN_DEPTH and self.rng.random() < FOLLOW_ON_RATE:
            next_arrival = end + self.rng.randint(10, 90) * 60
            events += self.patient_journey(next_arrival, transfer_to, patient_id, depth + 1)
        return events

    def scripted_journey(self, arrival, path, patient_id):
        """Passages successifs d'un patient le long de path, chacun se terminant par un transfert."""
        events, visit_ids, department = [], [], None
        for origin, destination in zip(path, path[1:]):
            new_events, end, department, visit_id = self.visit_events(
                arrival, origin, patient_id, department, transfer_to=destination
            )
            events += new_events
            visit_ids.append(visit_id)
            arrival = end + self.rng.randint(10, 90) * 60
        return events, visit_ids

    def emissions(self, events):
        """Associe a chaque evenement son instant d'emission (retards et doublons compris)."""
        out = []
        for event, ts in events:
            emit_at = ts
            if self.rng.random() < LATE_RATE:
                emit_at += self.rng.randint(LATE_MIN_SECONDS, LATE_MAX_SECONDS)
            out.append((emit_at, event))
            if self.rng.random() < DUPLICATE_RATE:
                out.append((emit_at + self.rng.randint(1, 120), event))
        return out

    def anomalies(self, start, span, scale):
        """Injecte des cas inhabituels et retourne (evenements, verite terrain)."""
        rng = self.rng
        events, truth = [], []
        groups = sorted(self.by_group)
        chus = {g: [f for f in self.by_group[g] if f["type"] == "CHU"] for g in groups}
        counts = {name: max(1, round(base * scale)) for name, base in ANOMALY_BASE_COUNTS.items()}

        def random_arrival():
            return start + rng.uniform(0, span * 0.9)

        for _ in range(counts["frequent_transfers"]):
            group = rng.choice(groups)
            path = rng.sample(self.by_group[group], rng.randint(5, 7))
            patient = self.pick_patient(path[0]["city"])
            new_events, visits = self.scripted_journey(random_arrival(), path, patient)
            events += new_events
            truth.append({"rule": "frequent_transfers", "patient_id": patient, "visit_ids": visits,
                          "facilities": [f["facility_id"] for f in path]})

        for _ in range(counts["transfer_loop"]):
            group = rng.choice(groups)
            a, b = rng.sample(self.by_group[group], 2)
            patient = self.pick_patient(a["city"])
            new_events, visits = self.scripted_journey(random_arrival(), [a, b, a], patient)
            events += new_events
            truth.append({"rule": "transfer_loop", "patient_id": patient, "visit_ids": visits,
                          "facilities": [a["facility_id"], b["facility_id"]]})

        for _ in range(counts["cross_group_non_chu"]):
            origin_group, target_group = rng.sample(groups, 2)
            origin = rng.choice(self.by_group[origin_group])
            target = rng.choice([f for f in self.by_group[target_group] if f["type"] != "CHU"])
            patient = self.pick_patient(origin["city"])
            new_events, visits = self.scripted_journey(random_arrival(), [origin, target], patient)
            events += new_events
            truth.append({"rule": "cross_group_non_chu", "patient_id": patient, "visit_ids": visits,
                          "facilities": [origin["facility_id"], target["facility_id"]]})

        # Un etablissement modeste recoit (ou envoie) beaucoup plus de transferts que ses pairs
        hub_group = groups[-1]
        receiver = rng.choice([f for f in self.by_group[hub_group] if f["type"] == "CLINIQUE"])
        sources = [f for f in self.by_group[hub_group] if f is not receiver]
        visits = []
        for _ in range(counts["receiver_hub"]):
            origin = rng.choice(sources)
            new_events, ids = self.scripted_journey(random_arrival(), [origin, receiver], self.pick_patient(origin["city"]))
            events += new_events
            visits += ids
        truth.append({"rule": "receiver_hub", "facility_id": receiver["facility_id"], "visit_ids": visits})

        sender_group = groups[0]
        sender = rng.choice([f for f in self.by_group[sender_group] if f["type"] == "CENTRE_SANTE"])
        targets = chus[sender_group] + [f for f in self.by_group[sender_group] if f["type"] == "HOPITAL_REGIONAL"]
        visits = []
        for _ in range(counts["sender_hub"]):
            new_events, ids = self.scripted_journey(
                random_arrival(), [sender, rng.choice(targets)], self.pick_patient(sender["city"])
            )
            events += new_events
            visits += ids
        truth.append({"rule": "sender_hub", "facility_id": sender["facility_id"], "visit_ids": visits})

        return events, truth


def diurnal_arrivals(rng, start, span, count):
    """Instants d'arrivee sur la periode, avec davantage de passages en journee."""
    arrivals = []
    while len(arrivals) < count:
        ts = start + rng.uniform(0, span)
        hour = (ts % 86400) / 3600
        if rng.random() < (1.0 if 8 <= hour < 22 else 0.4):
            arrivals.append(ts)
    return sorted(arrivals)


def run_file(args, facilities):
    rng = random.Random(args.seed)
    sim = Simulator(facilities, rng)
    start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()
    span = args.days * 86400

    events, truth = [], []
    if not args.no_anomalies:
        events, truth = sim.anomalies(start, span, args.n / 50_000)
    emissions = sim.emissions(events)

    # La marge couvre les ecarts au nombre moyen d'evenements ; on s'arrete des que n est atteint
    expected_visits = int(args.n / AVERAGE_EVENTS_PER_VISIT * 1.1) + 1
    for arrival in diurnal_arrivals(rng, start, span, expected_visits):
        if len(emissions) >= args.n:
            break
        emissions += sim.emissions(sim.patient_journey(arrival, sim.pick_facility()))

    emissions.sort(key=lambda item: item[0])
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", encoding="utf-8", newline="\n") as f:
        for _, event in emissions:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")

    if truth:
        ground_truth = Path(args.ground_truth)
        with ground_truth.open("w", encoding="utf-8", newline="\n") as f:
            json.dump(truth, f, ensure_ascii=False, indent=2)
        print(f"Verite terrain : {len(truth)} cas -> {ground_truth}")
    visits = len({event["visit_id"] for _, event in emissions})
    print(f"{len(emissions)} evenements, {visits} passages -> {output}")


def run_kafka(args, facilities):
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
        acks="all",
        linger_ms=50,
    )

    now = time.time()
    rng = random.Random(args.seed)
    # Plage d'identifiants propre a chaque lancement : une relance du producteur
    # ne doit pas produire de faux doublons (visit_id, status).
    sim = Simulator(facilities, rng, first_visit_id=(int(now // 60) % 9000 + 1) * 1_000_000)
    visits_per_second = args.rate / AVERAGE_EVENTS_PER_VISIT

    # Regime permanent des le depart : on simule les passages deja en cours, sinon aucun
    # CARE_STARTED ni DISCHARGED n'apparaitrait durant les premieres minutes de la demonstration.
    queue, sequence = [], 0
    arrival = now - MAX_VISIT_SECONDS
    while arrival < now:
        for emit_at, event in sim.emissions(sim.patient_journey(arrival, sim.pick_facility())):
            if emit_at >= now:
                queue.append((emit_at, sequence, event))
                sequence += 1
        arrival += rng.expovariate(visits_per_second)
    heapq.heapify(queue)

    deadline = now + args.duration if args.duration else None
    sent, late, last_report = 0, 0, now
    print(f"Publication sur {args.topic} ({args.bootstrap_servers}) a ~{args.rate} evenements/s", flush=True)
    try:
        while deadline is None or time.time() < deadline:
            now = time.time()
            while arrival <= now:
                for emit_at, event in sim.emissions(sim.patient_journey(arrival, sim.pick_facility())):
                    heapq.heappush(queue, (emit_at, sequence, event))
                    sequence += 1
                arrival += rng.expovariate(visits_per_second)

            while queue and queue[0][0] <= now:
                emit_at, _, event = heapq.heappop(queue)
                producer.send(args.topic, key=event["visit_id"], value=event)
                sent += 1
                delay = now - datetime.strptime(event["event_time"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp()
                if delay >= LATE_MIN_SECONDS:
                    late += 1
                    if args.log_late and delay >= args.log_late * 60:
                        print(f"[RETARD {delay / 60:.1f} min] {event['visit_id']} {event['status']} "
                              f"event_time={event['event_time']} emis a {format_time(now)}", flush=True)

            if now - last_report >= 10:
                rate = sent / (now - last_report)
                print(f"{format_time(now)} : {sent} evenements en {now - last_report:.0f}s "
                      f"({rate:.1f}/s), dont {late} en retard", flush=True)
                sent, late, last_report = 0, 0, now
            time.sleep(0.02)
    except KeyboardInterrupt:
        pass
    finally:
        producer.flush()
        producer.close()


def main():
    parser = argparse.ArgumentParser(description="Generateur d'evenements UrgenceNet.ma (donnees synthetiques)")
    parser.add_argument("--mode", choices=["file", "kafka"], required=True)
    parser.add_argument("--facilities", default=ROOT / "data" / "facilities.json")
    parser.add_argument("--seed", type=int, default=None, help="graine (par defaut : 42 en mode file, aleatoire en mode kafka)")

    file_mode = parser.add_argument_group("mode file")
    file_mode.add_argument("--n", type=int, default=50_000, help="nombre d'evenements (approche, passages complets)")
    file_mode.add_argument("--output", default=ROOT / "data" / "urgences_history.jsonl")
    file_mode.add_argument("--ground-truth", default=ROOT / "data" / "anomalies_ground_truth.json")
    file_mode.add_argument("--start", default="2026-10-15", help="premier jour de l'historique (AAAA-MM-JJ)")
    file_mode.add_argument("--days", type=int, default=14)
    file_mode.add_argument("--no-anomalies", action="store_true")

    kafka_mode = parser.add_argument_group("mode kafka")
    kafka_mode.add_argument("--rate", type=float, default=20, help="evenements par seconde")
    kafka_mode.add_argument("--bootstrap-servers", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"))
    kafka_mode.add_argument("--topic", default="urgences")
    kafka_mode.add_argument("--duration", type=int, default=0, help="duree en secondes (0 = sans limite)")
    kafka_mode.add_argument("--log-late", type=float, default=0, metavar="MIN",
                            help="journaliser les evenements emis avec au moins MIN minutes de retard")
    args = parser.parse_args()

    if args.seed is None and args.mode == "file":
        args.seed = 42

    facilities = load_facilities(args.facilities)
    if args.mode == "file":
        run_file(args, facilities)
    else:
        run_kafka(args, facilities)
    return 0


if __name__ == "__main__":
    sys.exit(main())
