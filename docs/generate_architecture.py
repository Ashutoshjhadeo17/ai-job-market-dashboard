from graphviz import Digraph
from pathlib import Path

# --------------------------------------------------
# Output Path
# --------------------------------------------------
DOCS_DIR = Path(__file__).resolve().parent
OUTPUT_FILE = DOCS_DIR / "architecture_diagram"

# --------------------------------------------------
# Create Diagram
# --------------------------------------------------
dot = Digraph("AI Job Market Architecture", format="png")
dot.attr(rankdir="LR", fontsize="12")

# Nodes
dot.node("A", "Ingestion\n(api/scraper/selenium)")
dot.node("B", "Processing\n(pandas_cleaner)")
dot.node("C", "Spark Analytics\n(job_market_analysis)")
dot.node("D", "Feature Engineering\n(features/)")
dot.node("E", "Analytics Layer\n(salary & insights)")
dot.node("F", "Output CSVs\n(output/)")
dot.node("G", "Streamlit Dashboard\n(dashboards/app.py)")

# Edges
dot.edge("A", "B")
dot.edge("B", "C")
dot.edge("C", "D")
dot.edge("D", "E")
dot.edge("E", "F")
dot.edge("F", "G")

# Render
dot.render(OUTPUT_FILE, cleanup=True)

print("✅ Architecture diagram generated:")
print(OUTPUT_FILE.with_suffix(".png"))
