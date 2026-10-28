"""Independent Decimal PageRank and exact relational-data audit (stdlib only)."""
import argparse
import json
import re
from decimal import Decimal, getcontext
from pathlib import Path

getcontext().prec = 60
D = Decimal


def reference(path):
    graph = {}
    for line in path.read_text().splitlines():
        page, links = line.split("\t", 1)
        page = page.strip()
        graph.setdefault(page, set()).update(x.strip() for x in links.split(",") if x.strip())
    for target in set().union(*graph.values()):
        graph.setdefault(target, set())
    pages = sorted(graph)
    n = D(len(pages))
    rank = {p: 1 / n for p in pages}
    history = []
    for i in range(1, 11):
        # Dense transition-matrix calculation, independent of MapReduce/RDD implementation.
        nxt = {}
        for dst in pages:
            incoming = sum((rank[src] * ((1 / D(len(graph[src]))) if dst in graph[src] else D(0))
                            if graph[src] else rank[src] / n for src in pages), D(0))
            nxt[dst] = D("0.15") / n + D("0.85") * incoming
        rank = nxt
        assert abs(sum(rank.values()) - 1) < D("1e-50")
        history.append(rank)
    return graph, history


def load_output(path):
    ranks = {}
    for line in path.read_text().splitlines():
        m = re.fullmatch(r"\(([^,()]+),([0-9]+\.[0-9]{10})\)", line)
        assert m, f"Invalid ten-decimal output: {line!r}"
        assert m[1] not in ranks, f"Duplicate page: {m[1]}"
        ranks[m[1]] = D(m[2])
    return ranks


def audit_hbase(root, logdir):
    data = json.loads((root / "data/relational.json").read_text())
    expected = {}
    for table, rows in data.items():
        for r in rows:
            key = r["S_Name"] if table == "Student" else r["C_No"] if table == "Course" else r["SC_Sno"] + "_" + r["SC_Cno"]
            for k, v in r.items():
                expected[table, key, "info:" + k] = v
    java_log = (logdir / "hbase-java.log").read_text()
    actual_java = {}
    for line in java_log.splitlines():
        if line.startswith("DATA\t"):
            _, table, key, col, value = line.split("\t", 4)
            actual_java[table, key, col] = value
    assert actual_java == expected, "Java relational snapshot differs from handout"
    checks = re.findall(r"(?:^|New value: )CHECK (\S+) PASS$", java_log, re.M)
    assert len(checks) == 20 and "HBASE_JAVA_CHECKS 20 PASS" in java_log, checks
    shell_log = (logdir / "hbase-shell.log").read_text()
    assert "HBASE_SHELL_COMPLETED" in shell_log
    assert not re.search(r"(^|\n)(ERROR|Error|Traceback)", shell_log)
    actual_shell = {}
    for table in data:
        body = re.split(rf"^SNAPSHOT_BEGIN {table}$", shell_log, maxsplit=1, flags=re.M)[1]
        body = re.split(rf"^SNAPSHOT_END {table}$", body, maxsplit=1, flags=re.M)[0]
        for line in body.splitlines():
            m = re.match(r"\s*(\S+)\s+column=(info:[^,]+),\s*timestamp=.+?,\s*value=(.*)", line)
            if m:
                actual_shell[table, m[1], m[2]] = m[3].rstrip()
    assert actual_shell == expected, f"Shell snapshot differs: missing={expected.keys()-actual_shell.keys()}"
    def section(name):
        body = re.split(rf"^SHELL_CHECK {name}$", shell_log, maxsplit=1, flags=re.M)[1]
        return re.split(r"^SHELL_CHECK ", body, maxsplit=1, flags=re.M)[0]
    assert all(x in section("family_scan") for x in ["Score:Math", "Score:Computer Science", "Score:English"])
    assert "Score:Math" in section("single_scan") and "Score:English" not in section("single_scan")
    assert "0 row(s)" in section("missing_is_zero_cells")
    assert "value=90" in section("modified_90")
    assert "0 row(s)" in section("deleted_column")
    assert "extra" not in section("deleted_family")
    assert "1 row(s)" in section("deleted_row_count_1")
    assert "0 row(s)" in section("truncate_count_0")
    assert "0 row(s)" in section("recreate_count_0")
    return {"verified": True, "java_checks": len(checks), "source_cells": len(expected),
            "java_shell_snapshots_equal": True, "row_counts": {k:len(v) for k,v in data.items()},
            "source_anomalies": ["Mary and Lisi share 2015003", "SC includes missing student ID 2015002"]}


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--results", type=Path, required=True)
    p.add_argument("--logs", type=Path, required=True)
    args = p.parse_args()
    root = Path(__file__).resolve().parent
    graph, history = reference(root / "data/graph.tsv")
    ref = history[-1]
    outputs = {kind: load_output(args.results / (kind + ".txt")) for kind in ("mr", "spark")}
    errors = {}
    for kind, ranks in outputs.items():
        assert ranks.keys() == ref.keys(), (kind, ranks.keys(), ref.keys())
        error = max(abs(ranks[k] - ref[k]) for k in ref)
        assert error <= D("0.00000000005"), (kind, error)
        assert abs(sum(ranks.values()) - 1) <= D("0.0000000005")
        errors[kind] = str(error)
    raw_errors = []
    for iteration, ref_iter in enumerate(history, 1):
        raw = {}
        for line in (args.results / "mr-iterations" / f"iter-{iteration:02d}.tsv").read_text().splitlines():
            page, rank, links = line.split("\t", 2)
            raw[page] = D(rank)
            assert set(filter(None, links.split(","))) == graph[page]
        assert raw.keys() == ref_iter.keys()
        raw_errors.append(max(abs(raw[k] - ref_iter[k]) for k in raw))
        assert raw_errors[-1] < D("1e-13")
    spark_history = json.loads((args.results / "spark-history.json").read_text())
    assert [x["iteration"] for x in spark_history] == list(range(1, 11))
    assert all(abs(x["rank_sum"] - 1) < 1e-12 and x["nodes"] == len(graph) for x in spark_history)
    run_id = (args.results / "hdfs-root.txt").read_text().strip().rsplit("/", 1)[-1]
    mr_log = (args.logs / f"mr-{run_id}.log").read_text()
    completed = re.findall(r"^COMPLETED (job_\S+) (\S+)$", mr_log, re.M)
    assert [name for job, name in completed] == ["lab3-graph", *[f"lab3-pagerank-{i}" for i in range(1, 11)], "lab3-format"]
    spark_log = (args.logs / f"spark-{run_id}.log").read_text()
    app = re.findall(r"SPARK_APPLICATION_ID (application_\S+)", spark_log)
    assert len(app) == 1
    applications = (args.results / "yarn-applications.txt").read_text()
    assert re.search(re.escape(app[0]) + r"[^\n]*FINISHED\s+SUCCEEDED", applications)
    for job, name in completed:
        aid = job.replace("job_", "application_")
        assert re.search(re.escape(aid) + r"[^\n]*FINISHED\s+SUCCEEDED", applications), aid
    report = {"verified": True, "run_id": run_id, "iterations": 10, "nodes": len(graph),
              "unique_edges": sum(map(len, graph.values())), "dangling_nodes": sorted(k for k,v in graph.items() if not v),
              "initialization": "1/N", "damping": "0.85", "reference": "60-digit Decimal dense transition matrix",
              "max_absolute_error_after_10dp": errors, "mr_spark_equal_after_10dp": outputs["mr"] == outputs["spark"],
              "mr_max_raw_error_all_iterations": str(max(raw_errors)),
              "output_rank_sums": {kind:str(sum(ranks.values())) for kind,ranks in outputs.items()},
              "mr_all_iterations_verified": True, "yarn_mr_jobs": [x[0] for x in completed], "spark_yarn_application": app[0],
              "ranking": [{"page": k, "rank": str(outputs["mr"][k])} for k in sorted(ref, key=lambda k: (-ref[k], k))],
              "hbase": audit_hbase(root, args.logs)}
    (args.results / "verification.json").write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n")
    (args.results / "reference.json").write_text(json.dumps([{k:str(v) for k,v in h.items()} for h in history], indent=2) + "\n")
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
