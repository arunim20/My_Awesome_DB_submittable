use anyhow::{Context, Result};
use clap::Parser;
use common::query::Query;
use db_config::DbContext;
use std::io::{BufRead, BufReader, Write};

use crate::{
    cli::CliOptions,
    io_setup::{setup_disk_io, setup_monitor_io},
};

pub mod cli;
pub mod disk;
pub mod io_setup;
pub mod ops;
pub mod row;
pub mod schema;
pub mod optimizer;

fn db_main() -> Result<()> {
    let cli_options = CliOptions::parse();
    let ctx = DbContext::load_from_file(cli_options.get_config_path())?;

    let (disk_in, mut disk_out) = setup_disk_io();
    let (monitor_in, mut monitor_out) = setup_monitor_io();

    let mut disk_buf = BufReader::new(disk_in);
    let mut monitor_buf = BufReader::new(monitor_in);

    // Read query from monitor
    let mut input_line = String::new();
    monitor_buf.read_line(&mut input_line)?;
    let mut query: Query = serde_json::from_str(&input_line)?;
    // eprintln!("Original Query: {:#?}", query);

    // Apply AST Optimizations (Filter Push-Down, Sort Pruning)
    query.root = crate::optimizer::optimize(query.root, &ctx)?;

    // Set dynamic memory budgets based on query tree depth
    let heavy_ops = crate::optimizer::max_concurrent_heavy_ops(&query.root);
    crate::ops::set_heavy_op_count(heavy_ops);
    eprintln!("Max concurrent heavy ops: {}", heavy_ops);

    // Block size from disk
    disk_out.write_all(b"get block-size\n")?;
    disk_out.flush()?;
    input_line.clear();
    disk_buf.read_line(&mut input_line)?;
    let block_size: usize = input_line.trim().parse()?;
    eprintln!("Block size: {}", block_size);

    disk_out.write_all(b"get anon-start-block\n")?;
    disk_out.flush()?;
    input_line.clear();
    disk_buf.read_line(&mut input_line)?;
    if let Ok(base) = input_line.trim().parse::<u64>() {
        crate::disk::set_anon_block_base(base);
    } else {
        // If the python script doesn't support it, default to a high safe value
        crate::disk::set_anon_block_base(1_000_000_000); // 1 Billion blocks = 4TB safety margin
    }


    // Memory limit from monitor
    monitor_out.write_all(b"get_memory_limit\n")?;
    monitor_out.flush()?;
    input_line.clear();
    monitor_buf.read_line(&mut input_line)?;
    let memory_limit_mb: u64 = input_line.trim().parse()?;
    eprintln!("Memory limit: {} MB", memory_limit_mb);

    // Begin output
    monitor_out.write_all(b"validate\n")?;
    monitor_out.flush()?;

    // Execute the full query tree, streaming each row to monitor
    ops::execute_op(
        &query.root,
        &ctx,
        &mut disk_out,
        &mut disk_buf,
        block_size,
        memory_limit_mb,
        &mut |row| {
            let mut line = String::new();
            for val in row {
                line.push_str(&row::format_value(val));
                line.push('|');
            }
            line.push('\n');
            monitor_out.write_all(line.as_bytes())?;
            Ok(())
        },
    )?;

    monitor_out.write_all(b"!\n")?;
    monitor_out.flush()?;

    Ok(())
}

fn main() -> Result<()> {
    db_main().with_context(|| "From Database")
}

