"""MaritimeIQ ops console theme — harbor navy, brass chrome, Fraunces/Sora."""
from __future__ import annotations

import streamlit as st

from maritimeiq.ui.atmosphere import render_atmosphere

THEME_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Fraunces:opsz,wght@9..144,500;9..144,650;9..144,700&family=Sora:wght@300;400;500;600;700&display=swap');

:root {
  --mq-bg0: #071018;
  --mq-bg1: #0f1c28;
  --mq-bg2: #152636;
  --mq-panel: rgba(15, 28, 40, 0.72);
  --mq-panel-solid: #121f2c;
  --mq-foam: #e6edf2;
  --mq-muted: #8aa0b2;
  --mq-brass: #c9a227;
  --mq-brass-dim: rgba(201, 162, 39, 0.35);
  --mq-teal: #2a8f8a;
  --mq-teal-dim: rgba(42, 143, 138, 0.35);
  --mq-danger: #c45c5c;
  --mq-ok: #3d9a6a;
  --mq-warn: #c9a227;
  --mq-hairline: rgba(201, 162, 39, 0.22);
  --mq-font-display: "Fraunces", "Georgia", serif;
  --mq-font-ui: "Sora", "Helvetica Neue", sans-serif;
}

html, body, [data-testid="stAppViewContainer"] {
  background: linear-gradient(165deg, var(--mq-bg0) 0%, var(--mq-bg1) 48%, #0c1822 100%) !important;
  color: var(--mq-foam);
  font-family: var(--mq-font-ui);
}

[data-testid="stHeader"] {
  background: transparent !important;
  border-bottom: none !important;
}

[data-testid="stToolbar"] {
  opacity: 0.35;
  transition: opacity 0.25s ease;
}
[data-testid="stToolbar"]:hover { opacity: 1; }

.block-container {
  padding-top: 1.1rem !important;
  max-width: 1400px;
  position: relative;
  z-index: 2;
}

/* Collapse Streamlit wrappers around fixed atmosphere / style injects */
div:has(> .mq-atmosphere) {
  height: 0 !important;
  min-height: 0 !important;
  margin: 0 !important;
  padding: 0 !important;
  border: none !important;
  overflow: visible !important;
}

h1, h2, h3, .mq-title {
  font-family: var(--mq-font-display) !important;
  font-weight: 650 !important;
  letter-spacing: -0.01em;
  color: var(--mq-foam) !important;
}

h4, h5, h6, .stMarkdown p, label, .stCaption, [data-testid="stWidgetLabel"] {
  font-family: var(--mq-font-ui) !important;
}

/* —— Atmosphere —— */
.mq-atmosphere {
  position: fixed;
  inset: 0;
  z-index: 0;
  pointer-events: none;
  overflow: hidden;
}

.mq-vignette {
  position: absolute;
  inset: 0;
  background:
    radial-gradient(ellipse 80% 60% at 50% 40%, transparent 20%, rgba(7, 16, 24, 0.55) 70%, rgba(7, 16, 24, 0.88) 100%),
    radial-gradient(ellipse 50% 40% at 70% 20%, rgba(42, 143, 138, 0.06), transparent 60%);
}

.mq-wave-bands {
  position: absolute;
  inset: 0;
  opacity: 0.07;
  background: repeating-linear-gradient(
    -8deg,
    transparent,
    transparent 28px,
    rgba(42, 143, 138, 0.5) 28px,
    rgba(42, 143, 138, 0.5) 29px
  );
}

.mq-grid {
  position: absolute;
  inset: 0;
  opacity: 0.04;
  background-image:
    linear-gradient(rgba(138, 160, 178, 0.8) 1px, transparent 1px),
    linear-gradient(90deg, rgba(138, 160, 178, 0.8) 1px, transparent 1px);
  background-size: 64px 64px;
}

.mq-routes {
  position: absolute;
  inset: 0;
  width: 100%;
  height: 100%;
  opacity: 0.85;
}

.mq-lane {
  stroke-dasharray: 14 18;
  animation: mq-dash 28s linear infinite;
  will-change: stroke-dashoffset;
}
.mq-lane-slow {
  animation-duration: 44s;
  animation-direction: reverse;
}

.mq-track-dash {
  animation: mq-dash 22s linear infinite;
  will-change: stroke-dashoffset;
}
.mq-track-dash-b {
  animation: mq-dash 32s linear infinite reverse;
  will-change: stroke-dashoffset;
}

@keyframes mq-dash {
  to { stroke-dashoffset: -320; }
}

/* CSS vessel pings (no SVG SMIL — Streamlit-safe) */
.mq-vessel {
  offset-rotate: 0deg;
}
.mq-v1 {
  offset-path: path("M80 520 C320 180 720 140 1180 320");
  animation: mq-along 42s linear infinite;
}
.mq-v2 {
  offset-path: path("M100 640 C400 420 760 480 1320 280");
  animation: mq-along 55s linear infinite;
  animation-delay: -18s;
}
.mq-v3 {
  offset-path: path("M60 280 C380 360 700 620 1280 540");
  animation: mq-along 48s linear infinite;
  animation-delay: -10s;
}
@keyframes mq-along {
  from { offset-distance: 0%; }
  to { offset-distance: 100%; }
}

/* Watermarks */
.mq-watermark {
  position: absolute;
  pointer-events: none;
  opacity: 0.06;
}

.mq-watermark-compass {
  right: 4%;
  bottom: 8%;
  animation: mq-compass-spin 80s linear infinite;
  transform-origin: center center;
}

.mq-watermark-mark {
  left: 50%;
  top: 42%;
  transform: translate(-50%, -50%);
  text-align: center;
  animation: mq-breathe 12s ease-in-out infinite;
  opacity: 0.05;
}

.mq-wordmark {
  font-family: var(--mq-font-display);
  font-size: clamp(3rem, 8vw, 6.5rem);
  font-weight: 700;
  letter-spacing: 0.04em;
  color: var(--mq-foam);
  line-height: 1;
}

.mq-wordmark-sub {
  font-family: var(--mq-font-ui);
  font-size: 0.75rem;
  letter-spacing: 0.28em;
  text-transform: uppercase;
  color: var(--mq-muted);
  margin-top: 0.6rem;
}

@keyframes mq-compass-spin {
  to { transform: rotate(360deg); }
}

@keyframes mq-breathe {
  0%, 100% { transform: translate(-50%, -50%) translateX(0); opacity: 0.045; }
  50% { transform: translate(-50%, -50%) translateX(18px); opacity: 0.07; }
}

/* Hero lanes strip */
.mq-hero-lanes {
  margin: 0.35rem 0 1rem;
  border-radius: 4px;
  overflow: hidden;
  border: 1px solid var(--mq-hairline);
  background: linear-gradient(90deg, rgba(15, 28, 40, 0.9), rgba(21, 38, 54, 0.55));
  position: relative;
  height: 64px;
}
.mq-hero-label {
  position: absolute;
  top: 8px;
  left: 14px;
  z-index: 1;
  font-family: var(--mq-font-ui);
  font-size: 0.65rem;
  letter-spacing: 0.14em;
  text-transform: uppercase;
  color: var(--mq-muted);
}
.mq-hero-lanes svg {
  width: 100%;
  height: 64px;
  display: block;
}
.mq-hero-dot-a {
  animation: mq-hero-drift-a 28s linear infinite;
}
.mq-hero-dot-b {
  animation: mq-hero-drift-b 36s linear infinite;
}
@keyframes mq-hero-drift-a {
  0% { transform: translate(0, 0); }
  50% { transform: translate(900px, -12px); }
  100% { transform: translate(0, 0); }
}
@keyframes mq-hero-drift-b {
  0% { transform: translate(0, 0); }
  50% { transform: translate(700px, -8px); }
  100% { transform: translate(0, 0); }
}

@media (prefers-reduced-motion: reduce) {
  .mq-lane, .mq-lane-slow, .mq-track-dash, .mq-track-dash-b,
  .mq-watermark-compass, .mq-watermark-mark,
  .mq-kpi, .mq-feed-item, .mq-rise,
  .mq-vessel, .mq-v1, .mq-v2, .mq-v3,
  .mq-hero-dot-a, .mq-hero-dot-b {
    animation: none !important;
  }
}

/* —— Bridge status bar —— */
.mq-bridge, .mq-status {
  display: flex;
  flex-wrap: wrap;
  gap: 0.55rem;
  align-items: center;
  padding: 0.75rem 1rem 0.85rem;
  margin-bottom: 0.85rem;
  border-radius: 6px;
  background: linear-gradient(180deg, rgba(18, 31, 44, 0.92), rgba(12, 24, 34, 0.88));
  border: 1px solid var(--mq-hairline);
  border-bottom: 2px solid var(--mq-brass);
  backdrop-filter: blur(12px);
  -webkit-backdrop-filter: blur(12px);
  box-shadow: 0 8px 32px rgba(0, 0, 0, 0.28);
  position: relative;
  z-index: 3;
  font-family: var(--mq-font-ui);
}

.mq-bridge strong, .mq-status strong, .mq-brand {
  font-family: var(--mq-font-display);
  font-weight: 650;
  font-size: 1.15rem;
  color: var(--mq-foam);
  letter-spacing: 0.02em;
}

.mq-brand-only {
  justify-content: flex-start;
  padding: 0.85rem 1.15rem 1rem;
}
.mq-brand-only .mq-brand {
  font-size: 1.75rem;
  font-weight: 700;
  letter-spacing: 0.03em;
  line-height: 1.1;
}

.mq-progress-caption {
  font-family: var(--mq-font-ui);
  font-size: 0.78rem;
  color: var(--mq-muted);
  margin: 0.15rem 0 0.65rem;
}

.mq-chip {
  font-size: 0.72rem;
  font-weight: 500;
  letter-spacing: 0.02em;
  padding: 0.28rem 0.65rem;
  border-radius: 4px;
  background: rgba(42, 143, 138, 0.12);
  border: 1px solid rgba(42, 143, 138, 0.28);
  color: var(--mq-foam);
}
.mq-chip.ok { border-color: rgba(61, 154, 106, 0.45); background: rgba(61, 154, 106, 0.12); }
.mq-chip.warn { border-color: var(--mq-brass-dim); background: rgba(201, 162, 39, 0.12); color: #e8d48a; }
.mq-chip.danger { border-color: rgba(196, 92, 92, 0.5); background: rgba(196, 92, 92, 0.14); }
.mq-chip.accent { border-color: var(--mq-brass-dim); color: #e8d48a; }

/* —— Glass panels —— */
div[data-testid="stVerticalBlockBorderWrapper"] > div,
[data-testid="stExpander"],
.stTabs [data-baseweb="tab-panel"] {
  backdrop-filter: blur(8px);
}

/* —— Pipeline rail —— */
.mq-rail-wrap {
  position: relative;
  padding-left: 0.35rem;
}

.mq-track {
  position: absolute;
  left: 10px;
  top: 8px;
  bottom: 8px;
  width: 2px;
  background: linear-gradient(180deg, var(--mq-teal), var(--mq-brass-dim), transparent);
  opacity: 0.55;
  border-radius: 2px;
  overflow: hidden;
}
.mq-track::after {
  content: "";
  position: absolute;
  left: 0;
  width: 100%;
  height: 40%;
  background: repeating-linear-gradient(
    180deg,
    var(--mq-brass) 0 4px,
    transparent 4px 10px
  );
  animation: mq-track-flow 3.5s linear infinite;
  opacity: 0.85;
}

@keyframes mq-track-flow {
  to { transform: translateY(100%); }
}

.mq-rail-step {
  padding: 0.55rem 0.7rem 0.55rem 1.15rem;
  margin: 0.35rem 0;
  border-radius: 4px;
  border-left: 3px solid transparent;
  background: rgba(15, 28, 40, 0.55);
  font-size: 0.88rem;
  font-family: var(--mq-font-ui);
  transition: border-color 0.25s ease, background 0.25s ease;
  position: relative;
  z-index: 1;
}
.mq-rail-step.done {
  border-left-color: var(--mq-ok);
  background: rgba(61, 154, 106, 0.08);
}
.mq-rail-step.active {
  border-left-color: var(--mq-brass);
  background: rgba(201, 162, 39, 0.1);
  box-shadow: inset 0 0 0 1px var(--mq-hairline);
}
.mq-rail-step.pending {
  border-left-color: rgba(138, 160, 178, 0.25);
  opacity: 0.72;
}

/* —— KPIs —— */
.mq-kpi {
  background: linear-gradient(145deg, rgba(18, 31, 44, 0.92), rgba(15, 28, 40, 0.75));
  border: 1px solid var(--mq-hairline);
  border-left: 3px solid var(--mq-teal);
  border-radius: 6px;
  padding: 0.95rem 1.05rem;
  margin-bottom: 0.5rem;
  backdrop-filter: blur(10px);
  -webkit-backdrop-filter: blur(10px);
  animation: mq-rise 0.55s ease both;
  box-shadow: 0 6px 20px rgba(0, 0, 0, 0.22);
}
.mq-kpi .label {
  font-size: 0.72rem;
  font-weight: 500;
  letter-spacing: 0.06em;
  text-transform: uppercase;
  color: var(--mq-muted);
  font-family: var(--mq-font-ui);
}
.mq-kpi .value {
  font-size: 1.65rem;
  font-weight: 650;
  margin-top: 0.25rem;
  font-family: var(--mq-font-display);
  color: var(--mq-foam);
}

@keyframes mq-rise {
  from { opacity: 0; transform: translateY(10px); }
  to { opacity: 1; transform: translateY(0); }
}

.mq-title {
  font-size: 1.85rem !important;
  margin-bottom: 0.15rem !important;
}
.mq-sub {
  color: var(--mq-muted) !important;
  font-size: 0.92rem !important;
  margin-bottom: 0.75rem !important;
  font-family: var(--mq-font-ui) !important;
  font-weight: 400;
}

/* —— Agent feed —— */
.mq-feed-item {
  font-size: 0.8rem;
  padding: 0.4rem 0.55rem;
  margin: 0.28rem 0;
  border-radius: 4px;
  border-left: 2px solid var(--mq-teal);
  background: rgba(15, 28, 40, 0.65);
  font-family: var(--mq-font-ui);
  animation: mq-rise 0.4s ease both;
}
.mq-feed-item .ts {
  font-family: ui-monospace, "SF Mono", Menlo, monospace;
  color: var(--mq-muted);
  font-size: 0.72rem;
}
.mq-feed-item .agent {
  color: var(--mq-brass);
  font-weight: 600;
  letter-spacing: 0.02em;
}
.mq-feed-item.fresh {
  animation: mq-pulse-border 1.8s ease 1;
}

@keyframes mq-pulse-border {
  0% { border-left-color: var(--mq-brass); background: rgba(201, 162, 39, 0.12); }
  100% { border-left-color: var(--mq-teal); background: rgba(15, 28, 40, 0.65); }
}

/* —— Streamlit chrome —— */
.stButton > button {
  border-radius: 5px;
  font-family: var(--mq-font-ui);
  font-weight: 500;
  border: 1px solid var(--mq-hairline);
  transition: border-color 0.2s ease, background 0.2s ease;
}
.stButton > button[kind="primary"],
.stButton > button[data-testid="baseButton-primary"] {
  background: linear-gradient(180deg, #3a9e98, var(--mq-teal));
  border-color: rgba(42, 143, 138, 0.6);
  color: #071018;
  font-weight: 600;
}
.stButton > button:hover {
  border-color: var(--mq-brass);
}

[data-baseweb="tab-list"] {
  gap: 0.25rem;
  border-bottom: 1px solid var(--mq-hairline) !important;
}
[data-baseweb="tab"] {
  font-family: var(--mq-font-ui);
  font-weight: 500;
}
[data-baseweb="select"] > div,
.stTextInput input, .stTextArea textarea {
  background: rgba(15, 28, 40, 0.85) !important;
  border-color: var(--mq-hairline) !important;
  font-family: var(--mq-font-ui) !important;
}

div[data-testid="stMetric"] {
  background: var(--mq-panel);
  border: 1px solid var(--mq-hairline);
  border-radius: 6px;
  padding: 0.6rem 0.85rem;
}

hr {
  border-color: var(--mq-hairline) !important;
  opacity: 0.5;
}
</style>
"""


def apply_theme() -> None:
    # st.html avoids markdown sanitizer DOM breakage (tippy TypeError on complex SVG).
    if hasattr(st, "html"):
        st.html(THEME_CSS)
    else:
        st.markdown(THEME_CSS, unsafe_allow_html=True)
    render_atmosphere()
