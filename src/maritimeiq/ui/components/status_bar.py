"""Top brand bar for the walkthrough console."""
from __future__ import annotations

import streamlit as st


def render_status_bar() -> None:
    st.markdown(
        """
        <div class="mq-bridge mq-status mq-brand-only">
          <strong class="mq-brand">MaritimeIQ</strong>
        </div>
        """,
        unsafe_allow_html=True,
    )
