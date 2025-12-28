use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn test_integer_comparisons() {
    let mut cmd = Command::cargo_bin("isenta").unwrap();
    cmd.arg("CREATE TABLE test_int (id INTEGER, value INTEGER)");
    cmd.assert().success();

    cmd = Command::cargo_bin("isenta").unwrap();
    cmd.arg("INSERT INTO test_int VALUES (1, 10)");
    cmd.assert().success();

    cmd = Command::cargo_bin("isenta").unwrap();
    cmd.arg("INSERT INTO test_int VALUES (2, 20)");
    cmd.assert().success();
    
    cmd = Command::cargo_bin("isenta").unwrap();
    cmd.arg("INSERT INTO test_int VALUES (3, 30)");
    cmd.assert().success();

    // Test >
    cmd = Command::cargo_bin("isenta").unwrap();
    cmd.arg("SELECT id FROM test_int WHERE value > 15");
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("2"))
        .stdout(predicate::str::contains("3"))
        .stdout(predicate::str::contains("1").not());
        
    // Test <
    cmd = Command::cargo_bin("isenta").unwrap();
    cmd.arg("SELECT id FROM test_int WHERE value < 25");
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("1"))
        .stdout(predicate::str::contains("2"))
        .stdout(predicate::str::contains("3").not());

    // Test >=
    cmd = Command::cargo_bin("isenta").unwrap();
    cmd.arg("SELECT id FROM test_int WHERE value >= 20");
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("2"))
        .stdout(predicate::str::contains("3"))
        .stdout(predicate::str::contains("1").not());

    // Test <=
    cmd = Command::cargo_bin("isenta").unwrap();
    cmd.arg("SELECT id FROM test_int WHERE value <= 20");
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("1"))
        .stdout(predicate::str::contains("2"))
        .stdout(predicate::str::contains("3").not());
}

#[test]
fn test_text_comparisons() {
    let mut cmd = Command::cargo_bin("isenta").unwrap();
    cmd.arg("CREATE TABLE test_text (id INTEGER, name TEXT)");
    cmd.assert().success();

    cmd = Command::cargo_bin("isenta").unwrap();
    cmd.arg("INSERT INTO test_text VALUES (1, 'apple')");
    cmd.assert().success();

    cmd = Command::cargo_bin("isenta").unwrap();
    cmd.arg("INSERT INTO test_text VALUES (2, 'banana')");
    cmd.assert().success();

    // Test =
    cmd = Command::cargo_bin("isenta").unwrap();
    cmd.arg("SELECT id FROM test_text WHERE name = 'apple'");
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("1"))
        .stdout(predicate::str::contains("2").not());

    // Test !=
    cmd = Command::cargo_bin("isenta").unwrap();
    cmd.arg("SELECT id FROM test_text WHERE name != 'apple'");
    cmd.assert()
        .success()
        .stdout(predicate::str::contains("2"))
        .stdout(predicate::str::contains("1").not());
}
