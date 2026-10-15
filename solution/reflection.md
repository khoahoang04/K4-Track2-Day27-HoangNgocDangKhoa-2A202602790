# Reflection (≤1 page)

**Student / Team:** K4-Track2-Day27-HoangNgocDangKhoa-2A202602790  
**Phase Results:** Practice: Score 50.0 (TPR 1.0, FPR 0.0) | Public: Score 48.18 (TPR 1.0, FPR 0.0) | Private: Score 32.41 (TPR 0.6481, FPR 0.0, Overage 0.0)

---

### 1. Which fault types were hardest to catch, and why?
The hardest fault types to catch were the **subtle-magnitude faults** in `data_batch` (correlated multi-signal distribution shifts) and AI infrastructure (`feature_materialization` and `embedding_batch`):
- **Multi-signal distribution shift (`data_batch`):** Single univariate checks against nominal 3-sigma bounds (`row_count`, `null_rate`, `mean_amount`, `staleness_min`) completely missed subtle shifts where each individual metric remained strictly within its valid range (e.g., public batch with `row_count=557 < 561.3`, `mean_amount=88.91 < 90.6`, `staleness_min=7.02 < 8.42`). Catching these required formulating a composite Euclidean distance normalized by standard deviation:
  $$\text{norm} = \sqrt{\left(\frac{\text{rows} - \mu_{\text{rows}}}{\Delta_{\text{rows}}}\right)^2 + \left(\frac{\text{mean} - \mu_{\text{mean}}}{\Delta_{\text{mean}}}\right)^2 + \left(\frac{\text{null}}{\text{null}_{\text{max}}}\right)^2 + \left(\frac{\text{stale}}{\text{stale}_{\text{max}}}\right)^2}$$
  Setting the boundary threshold to $\text{norm} > 1.30$ cleanly caught these multidimensional shifts without generating false alarms.
- **Natural jitter vs. Feature Drift (`feature_materialization`):** The clean training-serving distribution exhibits natural variance with mean shift sigma reaching up to $0.472$. Rigidly applying the historical baseline constant ($0.4095$) would trigger damaging False Positives (penalized at $-30\times\text{FPR}$). Calibrating the threshold to $0.65$ separated clean jitter from genuine feature skew ($\ge 0.970$).
- **Subtle Corpus Staleness & Centroid Drift (`embedding_batch`):** Edge cases in document aging (e.g., $48.3$ days) and centroid shift ($0.040$) lay just below the nominal 3-sigma baseline limits ($49.8$ days and $0.0435$). Tightening the decision boundaries to $44.0$ days and $0.0395$ allowed early detection while preserving an FPR of $0.0$.

### 2. What would you change about your cost/coverage tradeoff, if you had another pass?
- **Full-pass tool allocation strategy:** In real-time data pipelines, the free event payload lacks deep internal health indicators. Thus, spending exactly one targeted metered tool call per incoming event was the most mathematically sound decision. In the Public phase, 160 events cost $240$ credits against a $220$ budget ($9.09\%$ overage, $-1.82$ point penalty). However, skipping tool calls to save budget would miss faults, losing over $4$ points in TPR per omission. Accepting the small overage penalty guaranteed a perfect TPR ($1.0$) and a high score of $48.18$.
- **Prioritizing precision over aggressive recall:** In the Private phase, with a $320$ credit budget, the overage penalty was $0.0$. Since False Positives carry a steep penalty coefficient of $-0.30$ (equivalent to $-30$ points at $\text{FPR}=1.0$), tuning thresholds conservatively to maintain $\text{FPR} = 0.0$ protected the final score from degradation caused by noisy edge cases.
- **Future refinement:** If given another pass, I would implement lightweight payload heuristics (e.g., checking declared SLA metadata against known schema expectations) to conditionally skip expensive $2.0$-credit calls for ultra-high-frequency, low-variance streams when budget is severely constrained.

### 3. State & statistical mechanisms to improve via `ctx.state`
With additional development time, I would expand `ctx.state` to implement:
1. **Online Streaming Statistics (Welford's Algorithm):** Instead of relying purely on static historical baselines, accumulate online rolling estimates of mean, variance, and null rates within `ctx.state` to detect non-stationary stream drifts and trend shifts over time.
2. **Exponentially Weighted Moving Average (EWMA) & CUSUM:** Deploy Cumulative Sum control charts within `ctx.state` across consecutive batches. CUSUM is optimal for detecting small, persistent parameter shifts that single-batch thresholds fail to flag, enabling even higher subtle-fault TPR without increasing FPR.
3. **Producer-level & Model-level Reputation Tracking:** Track violation histories per producer or dbt model in `ctx.state` to dynamically adjust anomaly sensitivity for consistently healthy versus flaky pipeline components.
