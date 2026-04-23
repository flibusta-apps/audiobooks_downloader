use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use tempfile::tempdir;

fn write_dotenv(dir: &Path) -> PathBuf {
    let database_path = dir.join("startup-dotenv.sqlite");
    let database_url = format!("sqlite://{}?mode=rwc", database_path.display());

    std::fs::write(
        dir.join(".env"),
        format!(
            "APP_BIND_ADDR=127.0.0.1:0\nAPP_DATABASE_URL={database_url}\nAPP_STORAGE_ROOT=./storage\nAPP_API_KEY=from-dotenv\nAPP_DOWNLOAD_TTL_SECS=86400\nAPP_CLEANUP_INTERVAL_SECS=60\n"
        ),
    )
    .unwrap();

    database_path
}

fn base_command(dir: &Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_audiobooks_downloader"));
    command
        .current_dir(dir)
        .env_remove("APP_BIND_ADDR")
        .env_remove("APP_DATABASE_URL")
        .env_remove("APP_STORAGE_ROOT")
        .env_remove("APP_API_KEY")
        .env_remove("APP_DOWNLOAD_TTL_SECS")
        .env_remove("APP_CLEANUP_INTERVAL_SECS")
        .stdout(Stdio::null())
        .stderr(Stdio::null());
    command
}

fn shell_only_command(dir: &Path) -> Command {
    let database_path = dir.join("startup-shell-only.sqlite");
    let database_url = format!("sqlite://{}?mode=rwc", database_path.display());
    let mut command = base_command(dir);

    command
        .env("APP_BIND_ADDR", "127.0.0.1:0")
        .env("APP_DATABASE_URL", database_url)
        .env("APP_STORAGE_ROOT", "./storage")
        .env("APP_API_KEY", "from-shell")
        .env("APP_DOWNLOAD_TTL_SECS", "86400")
        .env("APP_CLEANUP_INTERVAL_SECS", "60");

    command
}

fn wait_for_database_and_running_process(mut child: Child, database_path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(3);

    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().unwrap() {
            panic!("process exited early with status {}", status);
        }

        if database_path.exists() {
            thread::sleep(Duration::from_millis(200));

            if let Some(status) = child.try_wait().unwrap() {
                panic!("process exited after startup with status {}", status);
            }

            child.kill().unwrap();
            let _ = child.wait().unwrap();
            return;
        }

        thread::sleep(Duration::from_millis(25));
    }

    child.kill().unwrap();
    let _ = child.wait().unwrap();
    panic!("database file was not created: {}", database_path.display());
}

#[test]
fn startup_loads_configuration_from_dotenv() {
    let dir = tempdir().unwrap();
    let database_path = write_dotenv(dir.path());

    let child = base_command(dir.path()).spawn().unwrap();
    wait_for_database_and_running_process(child, &database_path);
}

#[test]
fn shell_environment_overrides_dotenv_values() {
    let dir = tempdir().unwrap();
    let dotenv_database_path = write_dotenv(dir.path());
    let shell_database_path = dir.path().join("startup-shell.sqlite");
    let shell_database_url = format!("sqlite://{}?mode=rwc", shell_database_path.display());

    let mut command = base_command(dir.path());
    command.env("APP_DATABASE_URL", shell_database_url);

    let child = command.spawn().unwrap();
    wait_for_database_and_running_process(child, &shell_database_path);

    assert!(!dotenv_database_path.exists());
}

#[test]
fn missing_dotenv_is_ignored_when_shell_config_is_present() {
    let dir = tempdir().unwrap();
    let database_path = dir.path().join("startup-shell-only.sqlite");

    let child = shell_only_command(dir.path()).spawn().unwrap();
    wait_for_database_and_running_process(child, &database_path);
}

#[test]
fn malformed_dotenv_fails_startup() {
    let dir = tempdir().unwrap();
    std::fs::write(dir.path().join(".env"), "NOT A VALID LINE\nAPP_API_KEY=from-dotenv\n").unwrap();

    let status = shell_only_command(dir.path()).spawn().unwrap().wait().unwrap();

    assert!(!status.success());
    assert!(!dir.path().join("startup-shell-only.sqlite").exists());
}
