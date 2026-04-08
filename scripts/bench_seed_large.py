from __future__ import annotations

import argparse
import os
import random
import time
from collections import defaultdict

from neo4j import GraphDatabase
from neo4j.exceptions import AuthError


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Seed large synthetic KG for performance benchmarking.")
    parser.add_argument("--uri", default=os.getenv("NEO4J_URI", "bolt://127.0.0.1:7687"))
    parser.add_argument("--user", default=os.getenv("NEO4J_USER", "neo4j"))
    parser.add_argument("--password", default=os.getenv("NEO4J_PASSWORD", "neo4j"))
    parser.add_argument("--database", default=os.getenv("NEO4J_DATABASE", "neo4j"))
    parser.add_argument("--reset", action="store_true", help="Delete all nodes/relationships before seeding.")
    parser.add_argument("--num-person", type=int, default=20000)
    parser.add_argument("--num-city", type=int, default=2000)
    parser.add_argument("--num-country", type=int, default=200)
    parser.add_argument("--num-region", type=int, default=20)
    parser.add_argument("--num-company", type=int, default=5000)
    parser.add_argument("--num-university", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--batch-size", type=int, default=2000)
    parser.add_argument("--progress-every", type=int, default=10)
    return parser.parse_args()


def chunked(items: list[dict[str, str]], batch_size: int):
    for i in range(0, len(items), batch_size):
        yield items[i : i + batch_size]


def upsert_entities(
    session,
    label: str,
    ids: list[str],
    batch_size: int,
    progress_every: int,
) -> None:
    query = f"""
    UNWIND $rows AS row
    MERGE (n:{label} {{id: row.id}})
    """
    rows = [{"id": item} for item in ids]
    total_batches = (len(rows) + batch_size - 1) // batch_size
    for i, batch in enumerate(chunked(rows, batch_size), start=1):
        session.run(query, {"rows": batch}).consume()
        if i == 1 or i % progress_every == 0 or i == total_batches:
            print(f"[seed] entities {label}: batch {i}/{total_batches}", flush=True)


def upsert_rels(
    session,
    rel_type: str,
    pairs: list[tuple[str, str]],
    batch_size: int,
    progress_every: int,
) -> None:
    query = f"""
    UNWIND $rows AS row
    MATCH (s:Entity {{id: row.src}})
    MATCH (t:Entity {{id: row.dst}})
    MERGE (s)-[:`{rel_type}`]->(t)
    """
    rows = [{"src": s, "dst": t} for s, t in pairs]
    total_batches = (len(rows) + batch_size - 1) // batch_size
    for i, batch in enumerate(chunked(rows, batch_size), start=1):
        session.run(query, {"rows": batch}).consume()
        if i == 1 or i % progress_every == 0 or i == total_batches:
            print(f"[seed] rel {rel_type}: batch {i}/{total_batches}", flush=True)


def main() -> None:
    args = parse_args()
    rng = random.Random(args.seed)
    t0 = time.perf_counter()

    persons = [f"person_{i}" for i in range(args.num_person)]
    cities = [f"city_{i}" for i in range(args.num_city)]
    countries = [f"country_{i}" for i in range(args.num_country)]
    regions = [f"region_{i}" for i in range(args.num_region)]
    companies = [f"company_{i}" for i in range(args.num_company)]
    universities = [f"university_{i}" for i in range(args.num_university)]

    try:
        driver = GraphDatabase.driver(args.uri, auth=(args.user, args.password))
    except Exception as exc:
        raise SystemExit(f"[bench_seed_large] failed to create Neo4j driver: {exc}") from exc
    with driver.session(database=args.database) as session:
        try:
            if args.reset:
                print("[seed] reset graph...", flush=True)
                session.run("MATCH (n) DETACH DELETE n").consume()
            # Critical for large write throughput on relationship insertion.
            session.run(
                """
                CREATE CONSTRAINT entity_id_unique IF NOT EXISTS
                FOR (e:Entity) REQUIRE e.id IS UNIQUE
                """
            ).consume()
        except AuthError as exc:
            raise SystemExit(
                "[bench_seed_large] Neo4j authentication failed. "
                "Please pass correct credentials, e.g.:\n"
                "  python scripts/bench_seed_large.py "
                "--uri bolt://127.0.0.1:7687 --user neo4j --password <your-password> --database neo4j --reset\n"
                "or set env vars NEO4J_URI/NEO4J_USER/NEO4J_PASSWORD/NEO4J_DATABASE."
            ) from exc

        print("[seed] upserting entity nodes...", flush=True)
        upsert_entities(session, "Entity", persons, args.batch_size, args.progress_every)
        upsert_entities(session, "Entity", cities, args.batch_size, args.progress_every)
        upsert_entities(session, "Entity", countries, args.batch_size, args.progress_every)
        upsert_entities(session, "Entity", regions, args.batch_size, args.progress_every)
        upsert_entities(session, "Entity", companies, args.batch_size, args.progress_every)
        upsert_entities(session, "Entity", universities, args.batch_size, args.progress_every)

        city_to_country: dict[str, str] = {}
        country_to_region: dict[str, str] = {}
        for city in cities:
            city_to_country[city] = countries[rng.randrange(len(countries))]
        for country in countries:
            country_to_region[country] = regions[rng.randrange(len(regions))]

        born_in: list[tuple[str, str]] = []
        nationality: list[tuple[str, str]] = []
        no_nationality: list[tuple[str, str]] = []
        works_at: list[tuple[str, str]] = []
        educated_at: list[tuple[str, str]] = []
        lives_in: list[tuple[str, str]] = []
        person_to_country: dict[str, str] = {}

        print("[seed] generating synthetic relationship payloads...", flush=True)
        for person in persons:
            city = cities[rng.randrange(len(cities))]
            country = city_to_country[city]
            person_to_country[person] = country
            born_in.append((person, city))
            nationality.append((person, country))
            lives_in.append((person, city))
            works_at.append((person, companies[rng.randrange(len(companies))]))
            educated_at.append((person, universities[rng.randrange(len(universities))]))
            if rng.random() < 0.03:
                no_nationality.append((person, country))

        located_in = [(city, city_to_country[city]) for city in cities]
        part_of = [(country, country_to_region[country]) for country in countries]

        works_at_country: dict[str, set[str]] = defaultdict(set)
        for person, company in works_at:
            works_at_country[company].add(person_to_country[person])

        headquarters_in: list[tuple[str, str]] = []
        for company in companies:
            if company in works_at_country and works_at_country[company]:
                cands = list(works_at_country[company])
                headquarters_in.append((company, cands[rng.randrange(len(cands))]))
            else:
                headquarters_in.append((company, countries[rng.randrange(len(countries))]))

        print("[seed] upserting relationships...", flush=True)
        upsert_rels(session, "locatedIn", located_in, args.batch_size, args.progress_every)
        upsert_rels(session, "partOf", part_of, args.batch_size, args.progress_every)
        upsert_rels(session, "bornIn", born_in, args.batch_size, args.progress_every)
        upsert_rels(session, "nationality", nationality, args.batch_size, args.progress_every)
        upsert_rels(session, "noNationality", no_nationality, args.batch_size, args.progress_every)
        upsert_rels(session, "worksAt", works_at, args.batch_size, args.progress_every)
        upsert_rels(session, "educatedAt", educated_at, args.batch_size, args.progress_every)
        upsert_rels(session, "livesIn", lives_in, args.batch_size, args.progress_every)
        upsert_rels(session, "headquartersIn", headquarters_in, args.batch_size, args.progress_every)

    driver.close()
    elapsed = time.perf_counter() - t0
    print(f"[seed] completed in {elapsed:.2f}s", flush=True)


if __name__ == "__main__":
    main()
