// use anyhow::{Context, Result, bail};
// use clap::Parser;
// use libc::{rlimit64, setrlimit64};
// use monitor_config::{
//     MonitorConfig,
//     monitor_config::{DatabaseConfig, QueryConfig},
// };
// use std::{
//     fs::File,
//     io::{BufRead, BufReader, PipeReader, PipeWriter, Read, Write, pipe},
//     os::{fd::AsRawFd, unix::process::CommandExt},
//     path::PathBuf,
//     process::{Child, Command},
// };

// use crate::{
//     cli::CliOptions,
//     fd_mapper::{FdMapping, remap_fds},
// };

// mod cli;
// mod fd_mapper;

// const MAX_COMMAND_LENGTH: u64 = 1024;

// fn setup_disk_process(monitor_config: &MonitorConfig) -> Result<(Child, PipeReader, PipeWriter)> {
//     let (disk_outbound_reader, disk_outbound_writer) = pipe()?;
//     let (disk_inbound_reader, disk_inbound_writer) = pipe()?;

//     let disk_prog = &monitor_config.get_disk_config().disk_prog;
//     let disk_prog_config = &monitor_config.get_disk_config().disk_prog_config;

//     // Start disk program
//     let disk_process = Command::new(disk_prog)
//         .arg("--config")
//         .arg(disk_prog_config)
//         .stdin(disk_inbound_reader)
//         .stdout(disk_outbound_writer)
//         .spawn()?;

//     Ok((disk_process, disk_outbound_reader, disk_inbound_writer))
// }

// fn setup_db_process(
//     database_config: &DatabaseConfig,
//     query_config: &QueryConfig,
//     disk_outbound_reader: PipeReader,
//     disk_inbound_writer: PipeWriter,
// ) -> Result<(Child, PipeReader, PipeWriter)> {
//     let (monitor_to_db_reader, monitor_to_db_writer) = pipe()?;
//     let (db_to_monitor_reader, db_to_monitor_writer) = pipe()?;

//     let db_prog = &database_config.database_prog;
//     let db_prog_config = &database_config.database_prog_config;

//     // Setup database program
//     let mut db_process = Command::new(db_prog);
//     db_process.arg("--config").arg(db_prog_config);

//     let memory_limit = query_config.memory_limit_mb * 1024 * 1024;

//     unsafe {
//         db_process.pre_exec(move || {
//             remap_fds(&vec![
//                 FdMapping::new(disk_outbound_reader.as_raw_fd(), 3, false),
//                 FdMapping::new(disk_inbound_writer.as_raw_fd(), 4, false),
//                 FdMapping::new(monitor_to_db_reader.as_raw_fd(), 5, false),
//                 FdMapping::new(db_to_monitor_writer.as_raw_fd(), 6, false),
//             ]);

//             let mut rlimit = rlimit64 {
//                 rlim_cur: memory_limit,
//                 rlim_max: memory_limit,
//             };

//             if setrlimit64(libc::RLIMIT_AS, &rlimit) != 0 {
//                 panic!("Unable to set memory limit");
//             }

//             if setrlimit64(libc::RLIMIT_STACK, &rlimit) != 0 {
//                 panic!("Unable to set stack limit");
//             }

//             rlimit.rlim_cur = 0;
//             rlimit.rlim_max = 0;
//             if setrlimit64(libc::RLIMIT_FSIZE, &rlimit) != 0 {
//                 panic!("Unable to set max file size limit");
//             }

//             rlimit.rlim_cur = 1;
//             rlimit.rlim_max = 1;
//             if setrlimit64(libc::RLIMIT_NPROC, &rlimit) != 0 {
//                 panic!("Unable to set max processes limit");
//             }

//             Ok(())
//         });
//     }

//     let db_process_child = db_process.spawn()?;

//     Ok((db_process_child, db_to_monitor_reader, monitor_to_db_writer))
// }

// fn validate(db_in: &mut impl BufRead, expected_output_file_path: &PathBuf) -> Result<()> {
//     let mut expected_output_reader = BufReader::new(File::open(expected_output_file_path)?);
//     let mut line_count = 0;

//     loop {
//         line_count += 1;
//         let mut expected_output_line = String::new();
//         let mut db_in_line = String::new();

//         expected_output_reader.read_line(&mut expected_output_line)?;
//         db_in
//             .read_line(&mut db_in_line)
//             .context("Failed to read line from database output")?;

//         if expected_output_line.trim().len() == 0 {
//             if db_in_line.trim() != "!" {
//                 bail!("Expected end of output rows '!'\nbut found\n{}", db_in_line);
//             }
//             break;
//         }

//         if expected_output_line.trim() != db_in_line.trim() {
//             bail!(
//                 "Expected line output\n{}\nbut database returned\n{}\nerror at line {line_count}",
//                 expected_output_line,
//                 db_in_line
//             );
//         }
//     }

//     Ok(())
// }

// fn handle_db(
//     db_in: &mut impl BufRead,
//     db_out: &mut impl Write,
//     query_config: &QueryConfig,
// ) -> Result<()> {
//     // Write provide the query to db
//     db_out.write_all(format!("{}\n", serde_json::to_string(&query_config.query)?).as_bytes())?;
//     db_out.flush()?;

//     loop {
//         let command = read_command(db_in)?;
//         if command.len() == 0 {
//             break;
//         }

//         match command[0].to_lowercase().as_ref() {
//             "get_memory_limit" => {
//                 db_out.write_all(format!("{}\n", query_config.memory_limit_mb).as_bytes())?;
//             }
//             "validate" => {
//                 validate(db_in, &query_config.expected_output_file)?;
//                 return Ok(());
//             }
//             other => bail!("Unknown command: {other}"),
//         };
//     }

//     bail!("Program did not validate the result");
// }

// fn read_command<R: BufRead>(buf_reader: &mut R) -> Result<Vec<String>> {
//     loop {
//         let mut input_line = String::new();
//         let mut limited_reader = buf_reader.take(MAX_COMMAND_LENGTH);
//         let read_length = limited_reader
//             .read_line(&mut input_line)
//             .context("Failed to read input line")?;

//         let result: Vec<String> = input_line.split_whitespace().map(String::from).collect();

//         if !result.is_empty() || read_length == 0 {
//             return Ok(result);
//         }
//     }
// }

// fn monitor_main() -> Result<()> {
//     let cli_options = CliOptions::parse();

//     let monitor_config = MonitorConfig::load_config(cli_options.get_config_path())
//         .context("Failed to load monitor config")?;

//     for query_config in monitor_config.get_query_configs() {
//         if query_config.disabled {
//             continue;
//         }

//         let (mut disk_process, disk_outbound_reader, disk_inbound_writer) =
//             setup_disk_process(&monitor_config)?;

//         let (mut db_process, db_outbound_reader, mut db_inbound_writer) = setup_db_process(
//             monitor_config.get_database_config(),
//             query_config,
//             disk_outbound_reader,
//             disk_inbound_writer,
//         )?;

//         let mut db_in = BufReader::new(db_outbound_reader);
//         let db_result = handle_db(&mut db_in, &mut db_inbound_writer, &query_config);

//         // Prevent deadlock: explicit drop of pipes so the database doesn't block on a full pipe
//         // while the monitor waits for it to exit if validation failed.
//         drop(db_in);
//         drop(db_inbound_writer);

//         if db_result.is_err() {
//             let _ = db_process.kill();
//             let _ = disk_process.kill();
//         }

//         db_process.wait()?;
//         disk_process.wait()?;

//         println!(
//             "--------------------------------------------------------------------------------"
//         );
//         println!();

//         db_result.context(format!(
//             "Validation failed! for {}",
//             query_config.execution_name
//         ))?;

//         println!("Validation success! for {}", query_config.execution_name);
//     }

//     Ok(())
// }

// fn main() -> Result<()> {
//     monitor_main().with_context(|| "From Monitor")
// }


use anyhow::{Context, Result, bail};
use clap::Parser;
use libc::{rlimit64, setrlimit64};
use monitor_config::{
    MonitorConfig,
    monitor_config::{DatabaseConfig, QueryConfig},
};
use std::{
    fs::File,
    io::{BufRead, BufReader, PipeReader, PipeWriter, Read, Write, pipe},
    os::{fd::AsRawFd, unix::process::CommandExt},
    path::PathBuf,
    process::{Child, Command},
};

use crate::{
    cli::CliOptions,
    fd_mapper::{FdMapping, remap_fds},
};

mod cli;
mod fd_mapper;

const MAX_COMMAND_LENGTH: u64 = 1024;

fn setup_disk_process(monitor_config: &MonitorConfig) -> Result<(Child, PipeReader, PipeWriter)> {
    let (disk_outbound_reader, disk_outbound_writer) = pipe()?;
    let (disk_inbound_reader, disk_inbound_writer) = pipe()?;

    let disk_prog = &monitor_config.get_disk_config().disk_prog;
    let disk_prog_config = &monitor_config.get_disk_config().disk_prog_config;

    // Start disk program
    let disk_process = Command::new(disk_prog)
        .arg("--config")
        .arg(disk_prog_config)
        .stdin(disk_inbound_reader)
        .stdout(disk_outbound_writer)
        .spawn()?;

    Ok((disk_process, disk_outbound_reader, disk_inbound_writer))
}

fn setup_db_process(
    database_config: &DatabaseConfig,
    query_config: &QueryConfig,
    disk_outbound_reader: PipeReader,
    disk_inbound_writer: PipeWriter,
) -> Result<(Child, PipeReader, PipeWriter)> {
    let (monitor_to_db_reader, monitor_to_db_writer) = pipe()?;
    let (db_to_monitor_reader, db_to_monitor_writer) = pipe()?;

    let db_prog = &database_config.database_prog;
    let db_prog_config = &database_config.database_prog_config;

    // Setup database program
    let mut db_process = Command::new(db_prog);
    db_process.arg("--config").arg(db_prog_config);

    let memory_limit = query_config.memory_limit_mb * 1024 * 1024;

    unsafe {
        db_process.pre_exec(move || {
            remap_fds(&vec![
                FdMapping::new(disk_outbound_reader.as_raw_fd(), 3, false),
                FdMapping::new(disk_inbound_writer.as_raw_fd(), 4, false),
                FdMapping::new(monitor_to_db_reader.as_raw_fd(), 5, false),
                FdMapping::new(db_to_monitor_writer.as_raw_fd(), 6, false),
            ]);

            let mut rlimit = rlimit64 {
                rlim_cur: memory_limit,
                rlim_max: memory_limit,
            };

            if setrlimit64(libc::RLIMIT_AS, &rlimit) != 0 {
                panic!("Unable to set memory limit");
            }

            if setrlimit64(libc::RLIMIT_STACK, &rlimit) != 0 {
                panic!("Unable to set stack limit");
            }

            rlimit.rlim_cur = 0;
            rlimit.rlim_max = 0;
            if setrlimit64(libc::RLIMIT_FSIZE, &rlimit) != 0 {
                panic!("Unable to set max file size limit");
            }

            rlimit.rlim_cur = 1;
            rlimit.rlim_max = 1;
            if setrlimit64(libc::RLIMIT_NPROC, &rlimit) != 0 {
                panic!("Unable to set max processes limit");
            }

            Ok(())
        });
    }

    let db_process_child = db_process.spawn()?;

    Ok((db_process_child, db_to_monitor_reader, monitor_to_db_writer))
}

fn validate_bysorting(db_in: &mut impl BufRead, expected_output_file_path: &PathBuf) -> Result<()> {
    let mut expected_output_reader = BufReader::new(File::open(expected_output_file_path)?);
    let mut expected_rows = Vec::new();
    loop {
        let mut row = String::new();
        expected_output_reader
            .read_line(&mut row)
            .context("Failed to read row from expected output file")?;
        let trimmed_row = row.trim();
        if trimmed_row.len() == 0 {
            break;
        }
        expected_rows.push(trimmed_row.to_string());
    }

    let mut db_output_rows = Vec::new();

    loop {
        let mut db_in_line = String::new();
        db_in
            .read_line(&mut db_in_line)
            .context("Failed to read line from database output")?;

        if db_in_line.trim() == "!" {
            if db_output_rows.len() != expected_rows.len() {
                bail!(
                    "Number of rows didn't match, expected {} but found {}",
                    expected_rows.len(),
                    db_output_rows.len()
                );
            }
            break;
        }
        if db_output_rows.len() == expected_rows.len() {
            bail!(
                "Expected end of row `!`, but db outputed additional row {}",
                db_in_line
            );
        }
        if db_in_line.trim().len() == 0 {
            bail!("Empty line found");
        }
        db_output_rows.push(db_in_line.trim().to_string());
    }

    expected_rows.sort();
    db_output_rows.sort();

    for (expected_row, db_row) in expected_rows.iter().zip(db_output_rows.iter()) {
        if !expected_row.eq(db_row) {
            bail!(
                "Expected line output\n{}\nbut database returned\n{}",
                expected_row,
                db_row
            );
        }
    }

    Ok(())
}

fn validate_presorted(db_in: &mut impl BufRead, expected_output_file_path: &PathBuf) -> Result<()> {
    let mut expected_output_reader = BufReader::new(File::open(expected_output_file_path)?);
    let mut line_count = 0;

    loop {
        line_count += 1;
        let mut expected_output_line = String::new();
        let mut db_in_line = String::new();

        expected_output_reader.read_line(&mut expected_output_line)?;
        db_in
            .read_line(&mut db_in_line)
            .context("Failed to read line from database output")?;

        if expected_output_line.trim().len() == 0 {
            if db_in_line.trim() != "!" {
                bail!("Expected end of output rows '!'\nbut found\n{}", db_in_line);
            }
            break;
        }

        if expected_output_line.trim() != db_in_line.trim() {
            bail!(
                "Expected line output\n{}\nbut database returned\n{}\nerror at line {line_count}",
                expected_output_line,
                db_in_line
            );
        }
    }

    Ok(())
}

fn handle_db(
    db_in: &mut impl BufRead,
    db_out: &mut impl Write,
    query_config: &QueryConfig,
) -> Result<()> {
    // Write provide the query to db
    db_out.write_all(format!("{}\n", serde_json::to_string(&query_config.query)?).as_bytes())?;
    db_out.flush()?;

    loop {
        let command = read_command(db_in)?;
        if command.len() == 0 {
            break;
        }

        match command[0].to_lowercase().as_ref() {
            "get_memory_limit" => {
                db_out.write_all(format!("{}\n", query_config.memory_limit_mb).as_bytes())?;
            }
            "validate" => {
                match query_config.sort_before_check {
                    true => validate_bysorting(db_in, &query_config.expected_output_file),
                    false => validate_presorted(db_in, &query_config.expected_output_file),
                }?;
                return Ok(());
            }
            other => bail!("Unknown command: {other}"),
        };
    }

    bail!("Program did not validate the result");
}

fn read_command<R: BufRead>(buf_reader: &mut R) -> Result<Vec<String>> {
    loop {
        let mut input_line = String::new();
        let mut limited_reader = buf_reader.take(MAX_COMMAND_LENGTH);
        let read_length = limited_reader
            .read_line(&mut input_line)
            .context("Failed to read input line")?;

        let result: Vec<String> = input_line.split_whitespace().map(String::from).collect();

        if !result.is_empty() || read_length == 0 {
            return Ok(result);
        }
    }
}

fn monitor_main() -> Result<()> {
    let cli_options = CliOptions::parse();

    let monitor_config = MonitorConfig::load_config(cli_options.get_config_path())
        .context("Failed to load monitor config")?;

    let mut failures: Vec<(usize, String, String)> = Vec::new();
    let mut pass_count = 0;
    let total = monitor_config.get_query_configs().iter().filter(|q| !q.disabled).count();

    for (i, query_config) in monitor_config.get_query_configs().iter().enumerate() {
        let test_number = i + 1;
        if query_config.disabled {
            continue;
        }

        let (mut disk_process, disk_outbound_reader, disk_inbound_writer) =
            setup_disk_process(&monitor_config)?;

        let (mut db_process, db_outbound_reader, mut db_inbound_writer) = setup_db_process(
            monitor_config.get_database_config(),
            query_config,
            disk_outbound_reader,
            disk_inbound_writer,
        )?;

        let mut db_in = BufReader::new(db_outbound_reader);
        let db_result = handle_db(&mut db_in, &mut db_inbound_writer, &query_config);

        if let Err(_) = &db_result {
            let _ = db_process.kill();
        } else {
            let _ = db_process.wait();
        }

        let _ = disk_process.wait();

        println!(
            "--------------------------------------------------------------------------------"
        );
        println!();

        match db_result {
            Ok(()) => {
                pass_count += 1;
                println!("✓ PASS Test {}: {}", test_number, query_config.execution_name);
            }
            Err(e) => {
                let err_msg = format!("{:#}", e);
                eprintln!("✗ FAIL Test {}: {}", test_number, query_config.execution_name);
                eprintln!("  Error: {}", err_msg);
                failures.push((test_number, query_config.execution_name.clone(), err_msg));
            }
        }
    }

    // Print summary
    println!("\n================================================================================");
    println!("SUMMARY: {}/{} tests passed", pass_count, total);
    if !failures.is_empty() {
        println!("\nFailed tests:");
        for (num, name, err) in &failures {
            println!("  Test {}: {}", num, name);
            println!("    → {}", err);
        }

        // Write failures to file
        let fail_path = "failed_tests.txt";
        if let Ok(mut f) = std::fs::File::create(fail_path) {
            let _ = writeln!(f, "Failed tests: {}/{}", failures.len(), total);
            for (num, name, err) in &failures {
                let _ = writeln!(f, "\n--- Test {} ---", num);
                let _ = writeln!(f, "Query: {}", name);
                let _ = writeln!(f, "Error: {}", err);
            }
            println!("\nFailure details written to: {}", fail_path);
        }
    }
    println!("================================================================================");

    if !failures.is_empty() {
        bail!("{} tests failed out of {}", failures.len(), total);
    }

    Ok(())
}

fn main() -> Result<()> {
    monitor_main().with_context(|| "From Monitor")
}
