use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn test_select_with_where_clause() {
    let mut cmd = Command::cargo_bin("isenta").unwrap();
    cmd.arg("CREATE TABLE users (id INT, name TEXT)");
    cmd.assert().success();

    cmd = Command::cargo_bin("isenta").unwrap();
    cmd.arg("INSERT INTO users VALUES (1, 'Alice')");
    cmd.assert().success();

    cmd = Command::cargo_bin("isenta").unwrap();
    cmd.arg("INSERT INTO users VALUES (2, 'Bob')");
    cmd.assert().success();

    cmd = Command::cargo_bin("isenta").unwrap();
    cmd.arg("SELECT * FROM users WHERE name = 'Alice'");
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("1 | Alice"));

    cmd = Command::cargo_bin("isenta").unwrap();
    cmd.arg("SELECT name FROM users WHERE id = 2");
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Bob"));
}
