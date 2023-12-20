use pyroscope::{PyroscopeAgent, Result};
use pyroscope_pprofrs::{pprof_backend, PprofConfig};
use std::future::Future;
use std::time::Instant;
use tokio::task;
use tracing::info;

// `ProfilingHarness` provides an abstraction over the Pyroscope agent to
// facilitate profiling.
//
// # Examples
//
// ```
// let mut harness = ProfilingHarness::new_with_name("http://localhost:4040", "my-app");
// harness.start()?;
// harness.add_tag("Batch".to_string(), "first".to_string())?;
// harness.run(|| {
//     // Your code here
//     // (i.e. `your_function()` or `your_main()`)
// });
// harness.stop()?;
// ```
pub struct ProfilingHarness {
    server_url: String,
    service_name: String,
    tags: Vec<(&'static str, &'static str)>,
}

impl ProfilingHarness {
    pub fn new(server_url: &str, service_name: &str) -> Self {
        Self {
            server_url: server_url.to_string(),
            service_name: service_name.to_string(),
            tags: Vec::new(),
        }
    }

    pub fn add_tag(&mut self, key: &'static str, value: &'static str) -> &mut Self {
        self.tags.push((key, value));
        self
    }

    pub fn profile<F: FnOnce() -> ()>(&self, code_to_profile: F) -> Result<()> {
        let agent = PyroscopeAgent::builder(&self.server_url, &self.service_name)
            .backend(pprof_backend(PprofConfig::new().sample_rate(100)))
            .tags(self.tags.clone())
            .build()?;

        // Show start time
        let start = Instant::now();

        // Start Agent
        let agent_running = agent.start()?;

        // Execute the user's code
        code_to_profile();

        // Stop Agent
        let agent_ready = agent_running.stop()?;
        agent_ready.shutdown();

        info!("Elapsed time: {:?}", start.elapsed());

        Ok(())
    }

    pub async fn async_profile<F, Fut, E>(&self, code_to_profile: F) -> anyhow::Result<()>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = std::result::Result<(), E>>,
        E: std::fmt::Debug,
    {
        let agent = PyroscopeAgent::builder(&self.server_url, &self.service_name)
            .backend(pprof_backend(PprofConfig::new().sample_rate(100)))
            .tags(self.tags.clone())
            .build()?;

        // If the agent.start() is not async, we block in place.
        let agent_running = task::block_in_place(|| agent.start())?;

        let start = Instant::now();

        // Execute the user's code
        // Execute the user's code
        match code_to_profile().await {
            Ok(_) => (),
            Err(err) => {
                tracing::error!("Error occurred while profiling: {:?}", err);
                return Err(anyhow::anyhow!("Error occurred while profiling: {:?}", err));
            }
        }

        // If agent_running.stop() is not async, we block in place.
        let agent_ready = task::block_in_place(|| agent_running.stop())?;
        agent_ready.shutdown();

        info!("Elapsed time: {:?}", start.elapsed());

        Ok(())
    }
}
