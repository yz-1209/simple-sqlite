import os
import subprocess

import pytest


@pytest.fixture(autouse=True)
def remove_db_file():
    if os.path.isfile("users.db"):
        os.remove("users.db")


test_cases = [(".exit\n", "db > bye!\n"),
              ("insert 1 users1 users1@example.com\nselect\n.exit\n",
               "db > Executed.\ndb > Row(ID=1, Username=users1, Email=users1@example.com)\nExecuted.\ndb "
               "> bye!\n"),
              ("select\n.exit\n", "db > Executed.\ndb > bye!\n")]


@pytest.mark.parametrize("test_input,expected", test_cases)
def test_db(test_input, expected):
    db = subprocess.run(["./sqlite users.db warn"], shell=True, stdout=subprocess.PIPE, input=test_input.encode())
    assert db.returncode == 0
    assert db.stdout.decode() == expected


if __name__ == "__main__":
    pytest.main()
