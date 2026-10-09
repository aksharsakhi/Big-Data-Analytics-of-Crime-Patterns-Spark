# Contributing Guidelines - Review 3 Team

## Code Ownership
- **Scala Spark Core:** Sheela Akshar Sakhi
- **PySpark MLlib & Visuals:** Nishanth S Gowda

## Git Conventions
- Use conventional commits: `feat(...)`, `fix(...)`, `docs(...)`, `perf(...)`, `test(...)`.
- Verify code compiles before pushing:
  - Scala: `sbt compile`
  - Python: `python3 scripts/validate_dataset.py`
  - LaTeX: `/opt/homebrew/bin/tectonic report/report.tex`
