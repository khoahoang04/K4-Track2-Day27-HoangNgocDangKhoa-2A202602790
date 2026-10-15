"""
Data Siege - Online Fault Detector for Real-Time Data Pipelines.
Compliant with sandbox security requirements: no file I/O, stdlib only.
"""
import math
from api import Verdict


def register(ctx):
    ctx.on("data_batch", check_data_batch)
    ctx.on("contract_checkpoint", check_contract_checkpoint)
    ctx.on("lineage_run", check_lineage_run)
    ctx.on("feature_materialization", check_feature_materialization)
    ctx.on("embedding_batch", check_embedding_batch)


def check_data_batch(payload, ctx):
    """
    Pillar: checks
    Detects volume spikes/drops, customer_id null spikes, mean amount distribution shifts,
    freshness lag, and multi-signal composite Euclidean distance anomalies.
    """
    batch_id = payload.get("batch_id")
    profile = ctx.tools.batch_profile(batch_id)
    if "error" in profile:
        return Verdict(alert=False, pillar="checks", reason="tool_error")

    row_count = profile.get("row_count", 0)
    null_rate_dict = profile.get("null_rate", {})
    null_rate = null_rate_dict.get("customer_id", 0.0) if isinstance(null_rate_dict, dict) else 0.0
    mean_amount = profile.get("mean_amount", 0.0)
    staleness_min = profile.get("staleness_min", 0.0)

    # 1. Single-dimension bounds from calibrated 3-sigma baseline
    if row_count < ctx.baseline["row_count_min"]:
        return Verdict(alert=True, pillar="checks", reason=f"volume_drop_{row_count}")
    if row_count > ctx.baseline["row_count_max"]:
        return Verdict(alert=True, pillar="checks", reason=f"volume_spike_{row_count}")

    if null_rate > ctx.baseline["null_rate_max"]:
        return Verdict(alert=True, pillar="checks", reason=f"null_spike_{null_rate}")

    if mean_amount < ctx.baseline["mean_amount_min"]:
        return Verdict(alert=True, pillar="checks", reason=f"mean_amount_low_{mean_amount}")
    if mean_amount > ctx.baseline["mean_amount_max"]:
        return Verdict(alert=True, pillar="checks", reason=f"mean_amount_high_{mean_amount}")

    if staleness_min > ctx.baseline["staleness_min_max"]:
        return Verdict(alert=True, pillar="checks", reason=f"freshness_lag_{staleness_min}")

    # 2. Multi-signal Composite Distance:
    # Catches multi-dimensional correlated shifts where each metric is within 3-sigma
    # but the joint displacement is anomalously large.
    mu_rows = (ctx.baseline["row_count_min"] + ctx.baseline["row_count_max"]) / 2.0
    delta_rows = (ctx.baseline["row_count_max"] - ctx.baseline["row_count_min"]) / 2.0

    mu_mean = (ctx.baseline["mean_amount_min"] + ctx.baseline["mean_amount_max"]) / 2.0
    delta_mean = (ctx.baseline["mean_amount_max"] - ctx.baseline["mean_amount_min"]) / 2.0

    null_max = ctx.baseline["null_rate_max"]
    stale_max = ctx.baseline["staleness_min_max"]

    t_rows = abs(row_count - mu_rows) / delta_rows if delta_rows > 0 else 0.0
    t_mean = abs(mean_amount - mu_mean) / delta_mean if delta_mean > 0 else 0.0
    t_null = null_rate / null_max if null_max > 0 else 0.0
    t_stale = staleness_min / stale_max if stale_max > 0 else 0.0

    norm = math.sqrt(t_rows**2 + t_mean**2 + t_null**2 + t_stale**2)
    if norm > 1.30:
        return Verdict(alert=True, pillar="checks", reason=f"composite_shift_norm_{norm:.2f}")

    return Verdict(alert=False, pillar="checks")


def check_contract_checkpoint(payload, ctx):
    """
    Pillar: contracts
    Detects schema hash corruption, type violations (e.g. float to string),
    and SLA freshness delay violations.
    """
    contract_id = payload.get("contract_id")
    checkpoint_batch_id = payload.get("checkpoint_batch_id")
    diff = ctx.tools.contract_diff(contract_id, checkpoint_batch_id)
    if "error" in diff:
        return Verdict(alert=False, pillar="contracts", reason="tool_error")

    violations = diff.get("violations", [])
    if len(violations) > 0:
        return Verdict(alert=True, pillar="contracts", reason=f"violations_{','.join(violations)}")

    freshness_delay = diff.get("freshness_delay_min", 0.0)
    declared_sla = payload.get("declared_sla", {})
    declared_freshness = declared_sla.get("freshness_min") if isinstance(declared_sla, dict) else None

    if declared_freshness is not None and freshness_delay > declared_freshness:
        return Verdict(alert=True, pillar="contracts", reason=f"sla_violation_{freshness_delay}_gt_{declared_freshness}")

    if freshness_delay > ctx.baseline.get("freshness_delay_max_min", float("inf")):
        return Verdict(alert=True, pillar="contracts", reason=f"baseline_freshness_delay_{freshness_delay}")

    return Verdict(alert=False, pillar="contracts")


def check_lineage_run(payload, ctx):
    """
    Pillar: lineage
    Detects orphaned outputs (actual_downstream_count == 0), missing upstream dependencies
    (requiring both raw.orders and raw.customers for dbt:stg_orders), and execution runtime anomalies.
    """
    run_id = payload.get("run_id")
    slice_data = ctx.tools.lineage_graph_slice(run_id)
    if "error" in slice_data:
        return Verdict(alert=False, pillar="lineage", reason="tool_error")

    downstream_count = slice_data.get("actual_downstream_count", 0)
    if downstream_count == 0:
        return Verdict(alert=True, pillar="lineage", reason="orphan_output")

    actual_upstream = set(slice_data.get("actual_upstream", []))
    if not {"raw.orders", "raw.customers"}.issubset(actual_upstream):
        return Verdict(alert=True, pillar="lineage", reason="missing_upstream")

    duration_ms = slice_data.get("duration_ms", 0.0)
    if duration_ms > ctx.baseline.get("lineage_duration_ms_max", float("inf")):
        return Verdict(alert=True, pillar="lineage", reason=f"runtime_anomaly_{duration_ms}ms")

    return Verdict(alert=False, pillar="lineage")


def check_feature_materialization(payload, ctx):
    """
    Pillar: ai_infra
    Detects training-serving feature drift / skew.
    Calibrated threshold: 0.65 sigma cleanly separates natural clean jitter (up to 0.472)
    from actual distribution skews (>= 0.970) without raising false positives.
    """
    feature_view = payload.get("feature_view")
    batch_id = payload.get("batch_id")
    drift = ctx.tools.feature_drift(feature_view, batch_id)
    if "error" in drift:
        return Verdict(alert=False, pillar="ai_infra", reason="tool_error")

    mean_shift_sigma = drift.get("mean_shift_sigma")
    if mean_shift_sigma is None:
        train_std = drift.get("train_std", 1.0)
        if train_std > 0:
            mean_shift_sigma = abs(drift.get("serve_mean", 0.0) - drift.get("train_mean", 0.0)) / train_std
        else:
            mean_shift_sigma = 0.0

    if mean_shift_sigma > 0.65:
        return Verdict(alert=True, pillar="ai_infra", reason=f"feature_skew_sigma_{mean_shift_sigma}")

    return Verdict(alert=False, pillar="ai_infra")


def check_embedding_batch(payload, ctx):
    """
    Pillar: ai_infra
    Detects semantic centroid drift (threshold 0.0395) and knowledge corpus staleness
    (threshold 44.0 days), catching subtle drift/staleness while avoiding false positives.
    """
    corpus = payload.get("corpus")
    chunk_batch_id = payload.get("chunk_batch_id")
    drift = ctx.tools.embedding_drift(corpus, chunk_batch_id)
    if "error" in drift:
        return Verdict(alert=False, pillar="ai_infra", reason="tool_error")

    centroid_shift = drift.get("centroid_shift", 0.0)
    if centroid_shift > 0.0395:
        return Verdict(alert=True, pillar="ai_infra", reason=f"embedding_drift_{centroid_shift}")

    avg_doc_age_days = drift.get("avg_doc_age_days", 0.0)
    if avg_doc_age_days > 44.0:
        return Verdict(alert=True, pillar="ai_infra", reason=f"corpus_staleness_age_{avg_doc_age_days}")

    return Verdict(alert=False, pillar="ai_infra")
