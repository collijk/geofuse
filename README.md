# GeoFuse

[![PyPI](https://img.shields.io/pypi/v/geofuse?style=flat-square)](https://pypi.python.org/pypi/geofuse/)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/geofuse?style=flat-square)](https://pypi.python.org/pypi/geofuse/)
[![PyPI - License](https://img.shields.io/pypi/l/geofuse?style=flat-square)](https://pypi.python.org/pypi/geofuse/)

---

**Documentation**: [https://collijk.github.io/geofuse](https://collijk.github.io/geofuse)

**Source Code**: [https://github.com/collijk/geofuse](https://github.com/collijk/geofuse)

**PyPI**: [https://pypi.org/project/geofuse/](https://pypi.org/project/geofuse/)

---

GeoFuse leverages geocoding APIs, fuzzy string matching, and administrative boundary data to provide approximate geocoding for a data source.

## Installation

```sh
pip install geofuse
```

## Development

* Clone this repository
* Requirements:
  * [Poetry](https://python-poetry.org/)
  * Python 3.7+
* Create a virtual environment and install the dependencies

```sh
poetry install
```

### Testing

```sh
pytest
```

### Documentation

The documentation is automatically generated from the content of the [docs directory](./docs) and from the docstrings
 of the public signatures of the source code. The documentation is updated and published as a [Github project page
 ](https://pages.github.com/) automatically as part each release.

### Releasing

Trigger the [Draft release workflow](https://github.com/collijk/geofuse/actions/workflows/draft_release.yml)
(press _Run workflow_). This will update the changelog & version and create a GitHub release which is in _Draft_ state.

Find the draft release from the
[GitHub releases](https://github.com/collijk/geofuse/releases) and publish it. When
 a release is published, it'll trigger [release](https://github.com/collijk/geofuse/blob/master/.github/workflows/release.yml) workflow which creates PyPI
 release and deploys updated documentation.

### Pre-commit

Pre-commit hooks run all the auto-formatters (e.g. `black`, `isort`), linters (e.g. `mypy`, `flake8`), and other quality
 checks to make sure the changeset is in good shape before a commit/push happens.

You can install the hooks with (runs for each commit):

```sh
pre-commit install
```

Or if you want them to run only for each push:

```sh
pre-commit install -t pre-push
```

Or if you want e.g. want to run all checks manually for all files:

```sh
pre-commit run --all-files
```

---
