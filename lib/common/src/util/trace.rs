use super::time::elapsed_subsec;
use anyhow::Result;
use indicatif::ProgressStyle;
use tracing::instrument;
use tracing_indicatif::IndicatifLayer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

#[instrument]
pub fn initialize_tracing() -> Result<()> {
    let indicatif_layer = IndicatifLayer::new()
        .with_progress_style(
            ProgressStyle::with_template(
                "{span_child_prefix}{span_fields} -- {span_name} {wide_msg} {elapsed_subsec}",
            )
            .unwrap()
            .with_key("elapsed_subsec", elapsed_subsec),
        )
        .with_span_child_prefix_symbol("↳ ")
        .with_span_child_prefix_indent(" ");

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(indicatif_layer)
        .init();

    Ok(())
}
