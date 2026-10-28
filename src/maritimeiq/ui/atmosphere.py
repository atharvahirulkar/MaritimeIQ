"""Maritime atmosphere: shipping routes + watermarks (Streamlit-safe HTML/CSS)."""
from __future__ import annotations

import streamlit as st

# Keep markup simple: no SMIL, no SVG <text>, no HTML comments — Streamlit's
# markdown/DOM layer throws tippy TypeErrors on complex SVG trees.
ATMOSPHERE_HTML = """
<div class="mq-atmosphere" aria-hidden="true">
  <div class="mq-vignette"></div>
  <div class="mq-wave-bands"></div>
  <div class="mq-grid"></div>
  <svg class="mq-routes" viewBox="0 0 1440 900" preserveAspectRatio="xMidYMid slice" xmlns="http://www.w3.org/2000/svg">
    <defs>
      <linearGradient id="mqLaneGrad" x1="0%" y1="0%" x2="100%" y2="0%">
        <stop offset="0%" stop-color="#2a8f8a" stop-opacity="0"></stop>
        <stop offset="40%" stop-color="#2a8f8a" stop-opacity="0.5"></stop>
        <stop offset="100%" stop-color="#c9a227" stop-opacity="0"></stop>
      </linearGradient>
    </defs>
    <g opacity="0.16" stroke="#8aa0b2" stroke-width="0.6" fill="none">
      <line x1="120" y1="80" x2="1320" y2="80"></line>
      <line x1="120" y1="220" x2="1320" y2="220"></line>
      <line x1="120" y1="360" x2="1320" y2="360"></line>
      <line x1="120" y1="500" x2="1320" y2="500"></line>
      <line x1="120" y1="640" x2="1320" y2="640"></line>
      <line x1="180" y1="60" x2="180" y2="840"></line>
      <line x1="420" y1="60" x2="420" y2="840"></line>
      <line x1="720" y1="60" x2="720" y2="840"></line>
      <line x1="1020" y1="60" x2="1020" y2="840"></line>
      <line x1="1260" y1="60" x2="1260" y2="840"></line>
    </g>
    <path class="mq-lane" d="M80 520 C320 180 720 140 1180 320" fill="none" stroke="url(#mqLaneGrad)" stroke-width="1.6"></path>
    <path class="mq-lane mq-lane-slow" d="M100 640 C400 420 760 480 1320 280" fill="none" stroke="#2a8f8a" stroke-opacity="0.32" stroke-width="1.2"></path>
    <path class="mq-lane" d="M60 280 C380 360 700 620 1280 540" fill="none" stroke="#c9a227" stroke-opacity="0.26" stroke-width="1.3"></path>
    <path class="mq-lane mq-lane-slow" d="M200 760 C520 680 900 200 1360 160" fill="none" stroke="#2a8f8a" stroke-opacity="0.2" stroke-width="1.1"></path>
    <path class="mq-track-dash" d="M80 520 C320 180 720 140 1180 320" fill="none" stroke="#e6edf2" stroke-opacity="0.18" stroke-width="1" stroke-dasharray="6 14"></path>
    <path class="mq-track-dash-b" d="M100 640 C400 420 760 480 1320 280" fill="none" stroke="#c9a227" stroke-opacity="0.16" stroke-width="1" stroke-dasharray="4 12"></path>
    <circle class="mq-vessel mq-v1" cx="0" cy="0" r="3.2" fill="#c9a227" opacity="0.85"></circle>
    <circle class="mq-vessel mq-v2" cx="0" cy="0" r="2.6" fill="#2a8f8a" opacity="0.8"></circle>
    <circle class="mq-vessel mq-v3" cx="0" cy="0" r="2.8" fill="#e6edf2" opacity="0.5"></circle>
  </svg>
  <div class="mq-watermark mq-watermark-compass">
    <svg viewBox="0 0 200 200" width="260" height="260" xmlns="http://www.w3.org/2000/svg">
      <g fill="none" stroke="#c9a227" stroke-width="1.2">
        <circle cx="100" cy="100" r="88"></circle>
        <circle cx="100" cy="100" r="62"></circle>
        <circle cx="100" cy="100" r="28"></circle>
        <path d="M100 12 L108 100 L100 188 L92 100 Z" fill="#c9a227" fill-opacity="0.25"></path>
        <path d="M12 100 L100 92 L188 100 L100 108 Z" fill="#2a8f8a" fill-opacity="0.12" stroke="#2a8f8a"></path>
      </g>
    </svg>
  </div>
  <div class="mq-watermark mq-watermark-mark">
    <div class="mq-wordmark">MaritimeIQ</div>
    <div class="mq-wordmark-sub">Source to Silver to AI</div>
  </div>
</div>
"""

HERO_LANES_HTML = """
<div class="mq-hero-lanes" aria-hidden="true">
  <div class="mq-hero-label">Source to Semantics to Silver to AI</div>
  <svg viewBox="0 0 1200 56" preserveAspectRatio="none" xmlns="http://www.w3.org/2000/svg">
    <path class="mq-track-dash" d="M0 36 C200 8 400 48 600 22 S1000 6 1200 28" fill="none" stroke="#2a8f8a" stroke-width="1.4" stroke-dasharray="5 10" opacity="0.7"></path>
    <path class="mq-track-dash-b" d="M0 44 C240 24 480 54 720 32 S1020 16 1200 40" fill="none" stroke="#c9a227" stroke-width="1.2" stroke-dasharray="4 12" opacity="0.55"></path>
    <circle class="mq-hero-dot mq-hero-dot-a" cx="120" cy="30" r="2.5" fill="#c9a227"></circle>
    <circle class="mq-hero-dot mq-hero-dot-b" cx="420" cy="40" r="2.2" fill="#2a8f8a"></circle>
  </svg>
</div>
"""


def _inject(html: str) -> None:
    """Prefer st.html (no markdown sanitizer); fall back to markdown."""
    if hasattr(st, "html"):
        st.html(html)
    else:
        st.markdown(html, unsafe_allow_html=True)


def render_atmosphere() -> None:
    """Inject fixed atmosphere + watermark layers behind the console."""
    _inject(ATMOSPHERE_HTML)


def hero_lanes_svg() -> str:
    """Compact lanes strip for the Activation hero."""
    return HERO_LANES_HTML


def render_hero_lanes() -> None:
    _inject(HERO_LANES_HTML)
