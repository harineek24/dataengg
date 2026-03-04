import streamlit as st
from pathlib import Path
import re
import textwrap

st.set_page_config(
    page_title="GCP Data Engineering Portfolio",
    page_icon=":hammer_and_wrench:",
    layout="wide",
)

# ── Custom CSS for consistent styling ────────────────────────
st.markdown("""
<style>
    /* Tighter heading spacing */
    h1 { font-size: 2rem !important; margin-bottom: 0.5rem !important; }
    h2 { font-size: 1.5rem !important; margin-top: 1.5rem !important; }
    h3 { font-size: 1.2rem !important; margin-top: 1rem !important; }

    /* Project cards */
    .project-card {
        border: 1px solid #e0e0e0;
        border-radius: 12px;
        padding: 1.5rem;
        margin-bottom: 1rem;
        background: #fafafa;
    }
    .project-card h3 { margin-top: 0 !important; }

    /* Badge styling */
    .badge {
        display: inline-block;
        padding: 2px 10px;
        border-radius: 12px;
        font-size: 0.8rem;
        font-weight: 600;
        margin-right: 6px;
    }
    .badge-intermediate { background: #FFF3E0; color: #E65100; }
    .badge-advanced { background: #FCE4EC; color: #C62828; }
    .badge-service { background: #E3F2FD; color: #1565C0; }
</style>
""", unsafe_allow_html=True)

PROJECTS_DIR = Path(__file__).parent / "projects"

# ── Project registry ─────────────────────────────────────────
PROJECTS = {
    "API Rate Limiting & Analytics": {
        "dir": "api-rate-limiting-analytics",
        "difficulty": "Intermediate",
        "services": ["Cloud Run", "Firestore", "Cloud Monitoring"],
        "time": "~90 min",
        "tagline": "Serverless API gateway with transactional rate limiting and usage analytics",
    },
    "Data Pipeline Automation": {
        "dir": "data-pipeline-automation",
        "difficulty": "Intermediate",
        "services": ["BigQuery", "Cloud KMS", "Pub/Sub", "Cloud Functions"],
        "time": "~120 min",
        "tagline": "Self-monitoring encrypted pipeline with continuous queries and CMEK",
    },
    "Real-Time Streaming Pipeline": {
        "dir": "real-time-streaming-pipeline-dashboard",
        "difficulty": "Intermediate",
        "services": ["Pub/Sub", "Dataflow", "BigQuery", "Cloud Run"],
        "time": "~120 min",
        "tagline": "End-to-end IoT streaming with Beam windowed aggregations and live dashboard",
    },
    "ELT Pipeline Builder": {
        "dir": "elt-pipeline-builder",
        "difficulty": "Intermediate",
        "services": ["BigQuery", "Cloud Storage", "Cloud Composer"],
        "time": "~120 min",
        "tagline": "Interactive ELT wizard with DAG visualization and data lineage tracking",
    },
    "AI Model Bias Detection": {
        "dir": "ai-model-bias-detection",
        "difficulty": "Advanced",
        "services": ["Vertex AI", "Cloud Functions", "Cloud Scheduler", "Pub/Sub"],
        "time": "~90 min",
        "tagline": "Automated fairness monitoring with drift detection, severity-based alerting, and compliance audit trails",
    },
}


# ── Markdown parser → structured sections ────────────────────
def parse_markdown(filepath: Path) -> dict:
    """Parse a project markdown file into structured sections."""
    raw = filepath.read_text(encoding="utf-8")

    # Strip YAML frontmatter
    raw = re.sub(r"^---\s*\n.*?\n---\s*\n", "", raw, count=1, flags=re.DOTALL)

    sections = {}
    current_heading = "intro"
    current_content = []

    for line in raw.split("\n"):
        heading_match = re.match(r"^(#{1,3})\s+(.+)$", line)
        if heading_match:
            # Save previous section
            sections[current_heading] = "\n".join(current_content).strip()
            level = len(heading_match.group(1))
            title = heading_match.group(2).strip()
            current_heading = title
            current_content = []
        else:
            current_content.append(line)

    # Save last section
    sections[current_heading] = "\n".join(current_content).strip()

    return sections


def render_mermaid(mermaid_code: str, height: int = 500):
    """Render a zoomable/pannable mermaid diagram."""
    html = f"""
    <style>
        .zoom-container {{
            position: relative;
            width: 100%;
            height: {height - 40}px;
            overflow: hidden;
            border: 1px solid #e0e0e0;
            border-radius: 8px;
            background: #fafafa;
            cursor: grab;
        }}
        .zoom-container:active {{ cursor: grabbing; }}
        .zoom-inner {{
            transform-origin: 0 0;
            display: inline-block;
            padding: 1rem;
        }}
        .zoom-controls {{
            display: flex;
            gap: 6px;
            justify-content: flex-end;
            padding: 4px 0;
        }}
        .zoom-controls button {{
            background: #f5f5f5;
            border: 1px solid #ddd;
            border-radius: 6px;
            padding: 3px 10px;
            cursor: pointer;
            font-size: 0.8rem;
            color: #444;
        }}
        .zoom-controls button:hover {{ background: #e8e8e8; }}
    </style>
    <div class="zoom-controls">
        <button onclick="zoomBy(0.2)" title="Zoom in">+</button>
        <button onclick="zoomBy(-0.2)" title="Zoom out">&minus;</button>
        <button onclick="resetView()" title="Reset zoom">Reset</button>
    </div>
    <div class="zoom-container" id="zoomContainer">
        <div class="zoom-inner" id="zoomInner">
            <pre class="mermaid" style="background:transparent;">
{mermaid_code}
            </pre>
        </div>
    </div>
    <script type="module">
        import mermaid from 'https://cdn.jsdelivr.net/npm/mermaid@11/dist/mermaid.esm.min.mjs';
        mermaid.initialize({{
            startOnLoad: true,
            theme: 'base',
            themeVariables: {{
                fontSize: '14px',
                primaryColor: '#E3F2FD',
                primaryBorderColor: '#1565C0',
                lineColor: '#424242',
                secondaryColor: '#FFF3E0',
                tertiaryColor: '#E8F5E9'
            }}
        }});
    </script>
    <script>
        let scale = 1, tx = 0, ty = 0;
        let dragging = false, startX, startY;
        const container = document.getElementById('zoomContainer');
        const inner = document.getElementById('zoomInner');

        function applyTransform() {{
            inner.style.transform = 'translate(' + tx + 'px, ' + ty + 'px) scale(' + scale + ')';
        }}

        window.zoomBy = function(delta) {{
            scale = Math.min(Math.max(0.3, scale + delta), 3);
            applyTransform();
        }};

        window.resetView = function() {{
            scale = 1; tx = 0; ty = 0;
            applyTransform();
        }};

        container.addEventListener('wheel', function(e) {{
            e.preventDefault();
            const delta = e.deltaY > 0 ? -0.1 : 0.1;
            scale = Math.min(Math.max(0.3, scale + delta), 3);
            applyTransform();
        }}, {{ passive: false }});

        container.addEventListener('mousedown', function(e) {{
            dragging = true;
            startX = e.clientX - tx;
            startY = e.clientY - ty;
        }});

        container.addEventListener('mousemove', function(e) {{
            if (!dragging) return;
            tx = e.clientX - startX;
            ty = e.clientY - startY;
            applyTransform();
        }});

        container.addEventListener('mouseup', function() {{ dragging = false; }});
        container.addEventListener('mouseleave', function() {{ dragging = false; }});
    </script>
    """
    st.components.v1.html(html, height=height, scrolling=False)


def extract_mermaid(text: str):
    """Extract mermaid code from a markdown text block."""
    match = re.search(r"```mermaid\s*\n(.*?)```", text, re.DOTALL)
    return match.group(1).strip() if match else None


def render_code_blocks(text: str):
    """Render text, splitting out code blocks into st.code components."""
    # Split on code fences
    pattern = r"```(\w*)\s*\n(.*?)```"
    parts = re.split(pattern, text, flags=re.DOTALL)

    # parts: [text, lang, code, text, lang, code, ...]
    i = 0
    while i < len(parts):
        if i % 3 == 0:
            # Regular text
            chunk = parts[i].strip()
            if chunk:
                st.markdown(chunk)
        elif i % 3 == 2:
            # Code block — parts[i-1] is language, parts[i] is code
            lang = parts[i - 1] or "text"
            code = parts[i].strip()
            st.code(code, language=lang)
        i += 1


def render_section_content(title: str, content: str, use_expander: bool = False):
    """Render a section with proper Streamlit components."""
    # Check for mermaid diagrams first
    mermaid = extract_mermaid(content)
    if mermaid:
        # Remove mermaid block from content
        text_without_mermaid = re.sub(r"```mermaid\s*\n.*?```", "", content, flags=re.DOTALL).strip()
        if text_without_mermaid:
            st.markdown(text_without_mermaid)
        render_mermaid(mermaid, height=500)
        return

    if use_expander:
        with st.expander(title, expanded=False):
            render_code_blocks(content)
    else:
        render_code_blocks(content)


def render_steps(content: str):
    """Render numbered steps, each in its own expander."""
    # Split on numbered step headings like "1. **Create...**:"
    step_pattern = r"(\d+)\.\s+\*\*(.+?)\*\*:?\s*\n"
    parts = re.split(step_pattern, content)

    # parts[0] is intro text, then groups of (number, title, content)
    if parts[0].strip():
        st.markdown(parts[0].strip())

    i = 1
    while i + 2 < len(parts):
        step_num = parts[i]
        step_title = parts[i + 1]
        step_content = parts[i + 2].strip()
        with st.expander(f"Step {step_num}: {step_title}", expanded=False):
            render_code_blocks(step_content)
        i += 3


# ── Sidebar ──────────────────────────────────────────────────
st.sidebar.title("GCP DE Portfolio")
st.sidebar.caption("Google Cloud Professional Data Engineering")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Projects",
    ["Home"] + list(PROJECTS.keys()),
    label_visibility="collapsed",
)

# ── Home page ────────────────────────────────────────────────
if page == "Home":
    st.title("GCP Data Engineering Portfolio")
    st.markdown(
        "Hands-on project guides showcasing **Google Cloud Professional "
        "Data Engineering** skills. Each project includes architecture diagrams, "
        "production-ready code, deployment instructions, and extension challenges."
    )
    st.markdown("---")

    for name, info in PROJECTS.items():
        services_html = " ".join(
            f'<span class="badge badge-service">{s}</span>' for s in info["services"]
        )
        diff_class = "badge-advanced" if info["difficulty"] == "Advanced" else "badge-intermediate"
        st.markdown(
            f"""<div class="project-card">
                <h3>{name}</h3>
                <p>{info['tagline']}</p>
                <span class="badge {diff_class}">{info['difficulty']}</span>
                {services_html}
                <span style="float:right; color:#888; font-size:0.85rem;">{info['time']}</span>
            </div>""",
            unsafe_allow_html=True,
        )

# ── Project pages ────────────────────────────────────────────
else:
    project = PROJECTS[page]
    md_file = PROJECTS_DIR / project["dir"] / f"{project['dir']}.md"

    if not md_file.exists():
        st.error(f"Guide not found: `{md_file}`")
    else:
        sections = parse_markdown(md_file)

        # ── Title row ────────────────────────────────────────
        st.title(page)
        services_html = " ".join(
            f'<span class="badge badge-service">{s}</span>' for s in project["services"]
        )
        diff_class = "badge-advanced" if project["difficulty"] == "Advanced" else "badge-intermediate"
        st.markdown(
            f'<span class="badge {diff_class}">{project["difficulty"]}</span>'
            f'{services_html}'
            f'<span style="margin-left:12px; color:#888;">{project["time"]}</span>',
            unsafe_allow_html=True,
        )
        st.markdown("")

        # ── Problem & Solution ───────────────────────────────
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Problem")
            st.markdown(sections.get("Problem", ""))
        with col2:
            st.subheader("Solution")
            st.markdown(sections.get("Solution", ""))

        st.markdown("---")

        # ── Architecture Diagram ─────────────────────────────
        st.subheader("Architecture")
        arch_content = sections.get("Architecture Diagram", "")
        mermaid_code = extract_mermaid(arch_content)
        if mermaid_code:
            render_mermaid(mermaid_code, height=550)
        else:
            st.markdown(arch_content)

        st.markdown("---")

        # ── Prerequisites ────────────────────────────────────
        with st.expander("Prerequisites", expanded=False):
            render_code_blocks(sections.get("Prerequisites", ""))

        # ── Preparation ──────────────────────────────────────
        with st.expander("Preparation", expanded=False):
            render_code_blocks(sections.get("Preparation", ""))

        st.markdown("---")

        # ── Steps ────────────────────────────────────────────
        st.subheader("Implementation Steps")
        steps_content = sections.get("Steps", "")
        if steps_content:
            render_steps(steps_content)

        st.markdown("---")

        # ── Validation ───────────────────────────────────────
        st.subheader("Validation & Testing")
        render_code_blocks(sections.get("Validation & Testing", ""))

        st.markdown("---")

        # ── Cleanup, Discussion, Challenge in expanders ──────
        with st.expander("Cleanup", expanded=False):
            render_code_blocks(sections.get("Cleanup", ""))

        with st.expander("Discussion", expanded=False):
            render_code_blocks(sections.get("Discussion", ""))

        with st.expander("Extension Challenges", expanded=False):
            render_code_blocks(sections.get("Challenge", ""))
