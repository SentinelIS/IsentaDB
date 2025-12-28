use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn test_select_with_not_equals_clause() {
    let mut cmd = Command::cargo_bin("isenta").unwrap();
    cmd.arg("CREATE TABLE test_neq (id INT, name TEXT)");
    cmd.assert().success();

    cmd = Command::cargo_bin("isenta").unwrap();
    cmd.arg("INSERT INTO test_neq VALUES (1, 'Alice')");
    cmd.assert().success();

    cmd = Command::cargo_bin("isenta").unwrap();
    cmd.arg("INSERT INTO test_neq VALUES (2, 'Bob')");
    cmd.assert().success();

    cmd = Command::cargo_bin("isenta").unwrap();
    cmd.arg("INSERT INTO test_neq VALUES (3, 'Charlie')");
    cmd.assert().success();

    cmd = Command::cargo_bin("isenta").unwrap();
    cmd.arg("SELECT * FROM test_neq WHERE name != 'Bob'");
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("1 | Alice"))
        .stdout(predicate::str::contains("3 | Charlie"))
        .stdout(predicate::str::contains("Bob").not());
}

#[test]
fn test_update_with_not_equals_clause() {
    let mut cmd = Command::cargo_bin("isenta").unwrap();
    cmd.arg("CREATE TABLE test_update_neq (id INT, name TEXT)");
    cmd.assert().success();

    cmd = Command::cargo_bin("isenta").unwrap();
    cmd.arg("INSERT INTO test_update_neq VALUES (1, 'One')");
    cmd.assert().success();

    cmd = Command::cargo_bin("isenta").unwrap();
    cmd.arg("INSERT INTO test_update_neq VALUES (2, 'Two')");
    cmd.assert().success();

    cmd = Command::cargo_bin("isenta").unwrap();
    cmd.arg("UPDATE test_update_neq SET name = 'Changed' WHERE id != 2");
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Updated 1 rows in 'test_update_neq'"));

    cmd = Command::cargo_bin("isenta").unwrap();
    cmd.arg("SELECT name FROM test_update_neq WHERE id = 1");
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Changed"));
    
    cmd = Command::cargo_bin("isenta").unwrap();
    cmd.arg("SELECT name FROM test_update_neq WHERE id = 2");
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("Two"));
}
