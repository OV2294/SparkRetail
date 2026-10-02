"""Builds PNG charts from output/*.csv (written by modules 2-4)."""
import os
import pandas as pd, matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import config

O = config.OUTPUT_DIR
P = lambda n: os.path.join(O, n)
BLUE, ORANGE, GREEN, PURPLE, GREY = "#2b6cb0", "#dd6b20", "#2f855a", "#6b46c1", "#a0aec0"


def save(fig, name):
    fig.tight_layout(); fig.savefig(P(name), dpi=140); plt.close(fig)


def exists(*names): return all(os.path.exists(P(n)) for n in names)


if exists("q1_country_revenue.csv"):
    d = pd.read_csv(P("q1_country_revenue.csv")).iloc[::-1]
    fig, ax = plt.subplots(figsize=(8, 4.8)); ax.barh(d["country"], d["revenue"] / 1e6, color=BLUE)
    for y, (v, p) in enumerate(zip(d["revenue"] / 1e6, d["pct_of_total"])): ax.text(v, y, f" {p}%", va="center", fontsize=8)
    ax.set_xlabel("Revenue (GBP million)"); ax.set_title("Top 10 countries by revenue (UK = data skew)"); save(fig, "chart_country_revenue.png")
if exists("q3_monthly_trend.csv"):
    d = pd.read_csv(P("q3_monthly_trend.csv")); fig, ax = plt.subplots(figsize=(8.5, 4.5))
    ax.plot(d["invoice_month"], d["revenue"] / 1e3, marker="o", color=ORANGE)
    ax.annotate("Dec-2011: data ends 9 Dec", (len(d) - 1, d["revenue"].iloc[-1] / 1e3), textcoords="offset points", xytext=(-110, 15), fontsize=8)
    ax.set_ylabel("Revenue (GBP thousand)"); ax.set_title("Monthly revenue - Q4 2011 seasonal peak"); plt.xticks(rotation=45, ha="right"); save(fig, "chart_monthly_trend.png")
if exists("q2b_top_products_net.csv"):
    d = pd.read_csv(P("q2b_top_products_net.csv")).iloc[::-1]
    fig, ax = plt.subplots(figsize=(8.5, 4.8)); ax.barh(d["product"].str[:30], d["net_revenue"] / 1e3, color=GREEN)
    ax.set_xlabel("Net revenue (GBP thousand)"); ax.set_title("Top 10 products (net of cancellations)"); save(fig, "chart_top_products.png")
if exists("q5_weekday_hour.csv"):
    d = pd.read_csv(P("q5_weekday_hour.csv")); order = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
    d = d.set_index("weekday").reindex(order).fillna(0).reset_index()
    fig, ax = plt.subplots(figsize=(7, 4)); ax.bar(d["weekday"], d["revenue"] / 1e6, color=PURPLE)
    ax.set_ylabel("Revenue (GBP million)"); ax.set_title("Revenue by weekday (no Saturday trading)"); save(fig, "chart_weekday.png")
if exists("q6_cancellations.csv"):
    d = pd.read_csv(P("q6_cancellations.csv")); fig, ax = plt.subplots(figsize=(8, 4))
    ax.bar(d["invoice_month"], d["cancel_pct"], color=ORANGE); ax.set_ylabel("% of invoices cancelled"); ax.set_title("Cancellation rate per month")
    plt.xticks(rotation=45, ha="right"); save(fig, "chart_cancellations.png")
if exists("rfm_segments.csv"):
    d = pd.read_csv(P("rfm_segments.csv")); fig, ax = plt.subplots(1, 2, figsize=(10, 4))
    ax[0].bar(d["segment"], d["revenue"] / 1e6, color=BLUE); ax[0].set_title("Revenue by RFM segment (GBP m)")
    ax[1].bar(d["segment"], d["customers"], color=GREEN); ax[1].set_title("Customers per segment")
    for a in ax: a.tick_params(axis="x", rotation=30)
    save(fig, "chart_rfm.png")
if exists("skew_partitions.csv"):
    d = pd.read_csv(P("skew_partitions.csv")); fig, ax = plt.subplots(1, 2, figsize=(10, 4), sharey=True)
    ax[0].bar(d["partition"], d["before_salting"], color=ORANGE); ax[0].set_title("Records per partition - before salting")
    ax[1].bar(d["partition"], d["after_salting"], color=GREEN); ax[1].set_title("after salting the hot key (UK)")
    for a in ax: a.set_xlabel("partition")
    save(fig, "chart_skew.png")
if exists("storage_benchmark.csv"):
    d = pd.read_csv(P("storage_benchmark.csv")); fig, ax = plt.subplots(1, 3, figsize=(12, 4))
    lab = d["format"].str.replace(" (partitioned)", "\n(partitioned)", regex=False)
    for a, c, t in zip(ax, ["size_mb", "agg_read_s", "filter_read_s"], ["Disk size (MB)", "Aggregation read (s)", "Filtered read (s)"]):
        a.bar(lab, d[c].fillna(0), color=[GREY, BLUE, GREEN]); a.set_title(t)
    save(fig, "chart_storage_benchmark.png")
if exists("streaming_window_totals.csv"):
    d = pd.read_csv(P("streaming_window_totals.csv")); fig, ax = plt.subplots(figsize=(10, 4.5)); x = range(len(d))
    ax.bar(x, d["batch_sales"], color=GREY, label="batch truth (all events)")
    ax.bar(x, d["sales"], color=PURPLE, label="streaming (watermark drops late events)")
    ax.set_xticks(list(x)[::2]); ax.set_xticklabels(d["window_start"].str[5:][::2], rotation=60, ha="right", fontsize=7)
    ax.set_ylabel("Sales per tumbling window (GBP)"); ax.set_title("Streaming vs batch - tumbling-window sales"); ax.legend(); save(fig, "chart_streaming_windows.png")
print("Charts written to", O)
