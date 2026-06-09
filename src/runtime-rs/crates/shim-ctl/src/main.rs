// SPDX-License-Identifier: Apache-2.0
use anyhow::{Context, Result};
use common::{
    message::Message,
    types::{CheckpointRequest, ContainerConfig, ContainerID, ContainerProcess, TaskRequest},
};
use runtimes::RuntimeHandlerManager;
use tokio::sync::mpsc::channel;

const MESSAGE_BUFFER_SIZE: usize = 8;
const WORKER_THREADS: usize = 2;

async fn real_main() {
    let (sender, _receiver) = channel::<Message>(MESSAGE_BUFFER_SIZE);
    let manager = RuntimeHandlerManager::new("xxx", sender).unwrap();
    let req = TaskRequest::CreateContainer(ContainerConfig {
        container_id: "xxx".to_owned(),
        bundle: ".".to_owned(),
        rootfs_mounts: Vec::new(),
        terminal: false,
        options: None,
        stdin: None,
        stdout: Some("/tmp/hello.stdout".to_owned()),
        stderr: Some("/tmp/hello.stderr".to_owned()),
    });
    manager.handler_task_message(req).await.ok();
    let p = ContainerProcess::new("xxx", "").unwrap();
    manager.handler_task_message(TaskRequest::StartProcess(p.clone())).await.ok();

    let _ = p; // started above; this flow drives checkpoint/restore directly
    let cid = || ContainerID {
        container_id: "xxx".to_owned(),
    };
    // Mode + image path are env-selectable so ONE binary can checkpoint in VM-A and restore
    // in a fresh VM-B (cross-VM / migration-style restore). Default = the 3-cycle counter demo.
    let mode = std::env::var("SHIMCTL_MODE").unwrap_or_else(|_| "cycle".to_owned());
    let img = std::env::var("SHIMCTL_IMG").unwrap_or_else(|_| "/tmp/ckpt/xxx".to_owned());
    eprintln!(">>> shim-ctl: mode={mode} image_path={img}");

    match mode.as_str() {
        "checkpoint" => {
            tokio::time::sleep(std::time::Duration::from_secs(6)).await; // let the counter advance
            let r = manager
                .handler_task_message(TaskRequest::CheckpointContainer(CheckpointRequest {
                    container_id: cid(),
                    image_path: img.clone(),
                }))
                .await;
            eprintln!(">>> shim-ctl: CHECKPOINT {} (-> {img})", if r.is_ok() { "OK" } else { "FAIL" });
            tokio::time::sleep(std::time::Duration::from_secs(25)).await; // hold so the host can export the images
        }
        "restore" => {
            tokio::time::sleep(std::time::Duration::from_secs(3)).await; // fresh counter starts ~1
            let r = manager
                .handler_task_message(TaskRequest::RestoreContainer(CheckpointRequest {
                    container_id: cid(),
                    image_path: img.clone(),
                }))
                .await;
            eprintln!(">>> shim-ctl: RESTORE {} (<- {img})", if r.is_ok() { "OK" } else { "FAIL" });
            tokio::time::sleep(std::time::Duration::from_secs(150)).await; // hold for inspection
        }
        _ => {
            eprintln!(">>> shim-ctl: 3 checkpoint/restore cycles");
            for cycle in 1..=3u32 {
                tokio::time::sleep(std::time::Duration::from_secs(4)).await;
                let cp = manager
                    .handler_task_message(TaskRequest::CheckpointContainer(CheckpointRequest {
                        container_id: cid(),
                        image_path: img.clone(),
                    }))
                    .await;
                let rs = manager
                    .handler_task_message(TaskRequest::RestoreContainer(CheckpointRequest {
                        container_id: cid(),
                        image_path: img.clone(),
                    }))
                    .await;
                eprintln!(
                    ">>> shim-ctl: cycle {cycle}: checkpoint={} restore={}",
                    if cp.is_ok() { "OK" } else { "FAIL" },
                    if rs.is_ok() { "OK" } else { "FAIL" }
                );
            }
            tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            eprintln!(">>> shim-ctl: CYCLES DONE; holding 150s for inspection");
            tokio::time::sleep(std::time::Duration::from_secs(150)).await;
        }
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(WORKER_THREADS).enable_all().build()
        .context("prepare tokio runtime")?;
    runtime.block_on(real_main());
    Ok(())
}
