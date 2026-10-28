"""Produce a reproducible HBase Shell script from the exact handout data."""
import json
from pathlib import Path

root = Path(__file__).resolve().parent
data = json.loads((root / "data/relational.json").read_text())
lines = []
for table, rows in data.items():
    name = "lab3_shell_" + table
    lines += [f"disable '{name}' if exists '{name}'", f"drop '{name}' if exists '{name}'", f"create '{name}', 'info'"]
    for r in rows:
        key = r["S_Name"] if table == "Student" else r["C_No"] if table == "Course" else r["SC_Sno"] + "_" + r["SC_Cno"]
        for k, v in r.items():
            lines.append(f"put '{name}', '{key}', 'info:{k}', '{v}'")
lines += ["list", "describe 'lab3_shell_Student'", "describe 'lab3_shell_Course'", "describe 'lab3_shell_SC'"]
for table in data:
    name = "lab3_shell_" + table
    lines += [f"puts 'SNAPSHOT_BEGIN {table}'", f"scan '{name}'", f"puts 'SNAPSHOT_END {table}'", f"count '{name}'"]
s = "lab3_shell_Scratch"
lines += [
    f"disable '{s}' if exists '{s}'", f"drop '{s}' if exists '{s}'", f"create '{s}', 'info', 'Score'",
    f"put '{s}', 'Zhangsan', 'info:name', 'Zhangsan'", f"put '{s}', 'Mary', 'info:name', 'Mary'",
    f"put '{s}', 'Zhangsan', 'Score:Math', '86'", f"put '{s}', 'Zhangsan', 'Score:Computer Science', '77'",
    f"put '{s}', 'Zhangsan', 'Score:English', '69'", "puts 'SHELL_CHECK family_scan'", f"scan '{s}', {{COLUMNS => 'Score'}}",
    "puts 'SHELL_CHECK single_scan'", f"scan '{s}', {{COLUMNS => 'Score:Math'}}",
    "puts 'SHELL_CHECK missing_is_zero_cells'", f"get '{s}', 'Mary', {{COLUMN => 'Score:Math'}}",
    f"put '{s}', 'Zhangsan', 'Score:Math', '90'", "puts 'SHELL_CHECK modified_90'", f"get '{s}', 'Zhangsan', 'Score:Math'",
    f"deleteall '{s}', 'Zhangsan', 'Score:English'", "puts 'SHELL_CHECK deleted_column'", f"get '{s}', 'Zhangsan', 'Score:English'",
    f"alter '{s}', NAME => 'extra'", f"put '{s}', 'Mary', 'extra:note', 'temporary'", f"get '{s}', 'Mary', 'extra:note'",
    f"alter '{s}', NAME => 'extra', METHOD => 'delete'", "puts 'SHELL_CHECK deleted_family'", f"describe '{s}'",
    f"deleteall '{s}', 'Mary'", "puts 'SHELL_CHECK deleted_row_count_1'", f"count '{s}'",
    f"truncate '{s}'", "puts 'SHELL_CHECK truncate_count_0'", f"count '{s}'",
    f"put '{s}', 'old', 'info:name', 'old'", f"disable '{s}'", f"drop '{s}'", f"create '{s}', 'info', 'Score'",
    "puts 'SHELL_CHECK recreate_count_0'", f"count '{s}'", "puts 'HBASE_SHELL_COMPLETED'", "exit"
]
(root / "lab3_commands.rb").write_text("\n".join(lines) + "\n")
print(root / "lab3_commands.rb")
