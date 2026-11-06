import shutil
from datetime import date

import pytest

from minilakehouse.dag import load_models
from minilakehouse.generate import generate
from minilakehouse.warehouse import connect

START = date(2026, 10, 27)


@pytest.fixture(scope="session")
def landing_4days(tmp_path_factory):
    """Four days of generated landing files, shared read-only across tests."""
    home = tmp_path_factory.mktemp("src4")
    summary = generate(home, days=4, start=START, seed=11)
    return home, summary


@pytest.fixture
def home(landing_4days, tmp_path):
    """A fresh lakehouse home with a private copy of the landing files."""
    src, _ = landing_4days
    shutil.copytree(src / "landing", tmp_path / "landing")
    return tmp_path


@pytest.fixture
def con(home):
    c = connect(home)
    yield c
    c.close()


@pytest.fixture(scope="session")
def models():
    return load_models()


@pytest.fixture
def mem():
    c = connect(":memory:")
    yield c
    c.close()
