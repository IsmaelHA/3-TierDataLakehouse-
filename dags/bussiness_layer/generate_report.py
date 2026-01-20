import io
import os
from pathlib import Path
import matplotlib.pyplot as plt
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader
from reportlab.lib import colors
from ducklake_utils import GOLD_MITMA_TABLE

def get_day_type_name(dt):
    mapping = {
        0: "Sunday", 1: "Monday", 2: "Tue-Thu (Standard)", 
        5: "Friday", 6: "Saturday", 8: "Holiday"
    }
    return mapping.get(dt, f"Unknown ({dt})")

def create_mobility_plot(x_data, y_data, title, y_label, color='#007acc'):
    """
    Helper function to generate a plot and return it as an in-memory buffer.
    """
    plt.figure(figsize=(8, 3)) 
    
    plt.plot(x_data, y_data, marker='o', linestyle='-', color=color, linewidth=2)
    plt.fill_between(x_data, y_data, color=color, alpha=0.1)
    
    plt.title(title, fontsize=10)
    plt.ylabel(y_label, fontsize=9)
    plt.xlabel("Hour of Day", fontsize=9)
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.xticks(range(0, 24), fontsize=8)
    plt.yticks(fontsize=8)
    plt.tight_layout()
    
    buf = io.BytesIO()
    plt.savefig(buf, format='png', dpi=100)
    buf.seek(0)
    plt.close() 
    
    return buf


def _resolve_output_dir(output_dir: str) -> Path:
    """Resolve the output directory.

    If output_dir is relative, it is resolved under AIRFLOW_HOME when available.
    Always creates the directory.
    """
    p = Path(output_dir)
    if not p.is_absolute():
        airflow_home = Path(os.environ.get("AIRFLOW_HOME", "."))
        p = airflow_home / p
    p.mkdir(parents=True, exist_ok=True)
    return p


def _escape_ident_for_pragma(name: str) -> str:
    # PRAGMA table_info('<name>') uses a string literal, so we just escape single quotes.
    return (name or "").replace("'", "''")


def _table_has_column(con, table_name: str, column_name: str) -> bool:
    """Return True if `table_name` has `column_name`.

    Works for DuckDB/DuckLake connections.
    """
    try:
        t = _escape_ident_for_pragma(table_name)
        df_cols = con.execute(f"PRAGMA table_info('{t}')").fetch_df()
        if df_cols is None or df_cols.empty:
            return False
        return column_name in df_cols["name"].tolist()
    except Exception:
        return False


def generate_mobility_report_local(
    con,
    target_origins: list,
    output_dir: str = "include/outputs",
    pdf_filename: str = "mobility_report.pdf",
):
    """Generate the mobility report and save it locally.

    Outputs:
      - PDF report (pdf_filename)
      - CSV with the aggregated data (same name, .csv)

    Files are written under `output_dir` (default: include/outputs). If `output_dir`
    is a relative path, it is resolved under AIRFLOW_HOME when available.
    """

    # IMPORTANT CHANGE:
    # `target_origins` is now interpreted as a list of DISTRICTS (district_id),
    # and the filter is applied via JOIN with gold_geometry_wgs84:
    #   g.origin_zone (census_section_id) -> geo.census_section_id -> geo.district_id IN (...)

    if not target_origins:
        print("⚠️ Empty district list. Report skipped.")
        return

    print(f"📊 Generating Aggregated Report for Districts: {target_origins}")

    # 1. Prepare SQL with Dynamic List of District IDs
    placeholders = ', '.join(['?'] * len(target_origins))
    
    has_year_g = _table_has_column(con, GOLD_MITMA_TABLE, "year")
    has_year_geo = _table_has_column(con, "gold_geometry_wgs84", "year")
    year_join = " AND geo.year = g.year" if (has_year_g and has_year_geo) else ""

    query = f"""
        SELECT
            g.day_type,
            g.hour_period,
            SUM(g.total_trips) AS total_trips,
            AVG(g.total_trips) AS avg_trips,
            STDDEV_SAMP(g.total_trips) AS std_trips,
            AVG(g.num_days_observed) AS num_days_observed
        FROM {GOLD_MITMA_TABLE} g
        JOIN gold_geometry_wgs84 geo
          ON (
              geo.census_section_id = g.origin_zone
              OR geo.district_id = g.origin_zone
          )
         {year_join}
        WHERE geo.district_id IN ({placeholders})
        GROUP BY g.day_type, g.hour_period
        ORDER BY g.day_type, g.hour_period
    """
    
    df = con.execute(query, target_origins).fetch_df()
    
    if df.empty:
        print("⚠️ No data found for these districts. Report skipped.")
        return

    out_dir = _resolve_output_dir(output_dir)
    pdf_path = out_dir / pdf_filename
    csv_path = pdf_path.with_suffix(".csv")

    # --- Save CSV Data Locally ---
    try:
        print(f"💾 Saving CSV data to: {csv_path}")
        df.to_csv(csv_path, index=False)
        print("✅ CSV saved.")
    except Exception as e:
        print(f"❌ Failed to save CSV: {e}")
        # Don't raise: still try to generate the PDF


    # 2. Setup PDF Buffer
    print("📄 Generating PDF...")
    pdf_buffer = io.BytesIO()
    c = canvas.Canvas(pdf_buffer, pagesize=A4)
    width, height = A4
    
    # --- Title Page ---
    c.setFont("Helvetica-Bold", 24)
    c.drawString(50, height - 80, "MITMA Mobility Analysis Report")
    
    c.setFont("Helvetica", 12)
    c.setFillColor(colors.darkgray)
    c.drawString(50, height - 105, f"Aggregated Report for District IDs: {', '.join(map(str, target_origins))}")
    c.setFillColor(colors.black)
    
    y_position = height - 160
    unique_days = df['day_type'].unique()
    
    # 3. Iterate Through Each Day Type
    for dt in unique_days:
        day_name = get_day_type_name(dt)
        day_data = df[df['day_type'] == dt]
        
        # Check space for Text + 2 Plots (approx 550 units)
        if y_position < 550:
            c.showPage()
            y_position = height - 50 
        
        # --- A. Draw Text Stats ---
        c.setFont("Helvetica-Bold", 16)
        c.drawString(50, y_position, day_name)
        
        total_vol = day_data['total_trips'].sum()
        peak_row = day_data.loc[day_data['total_trips'].idxmax()]
        avg_std = day_data['std_trips'].mean()
        avg_obs = day_data['num_days_observed'].mean()

        c.setFont("Helvetica", 10)
        c.drawString(50, y_position - 25, f"• Total Daily Volume: {int(total_vol):,}")
        c.drawString(50, y_position - 40, f"• Peak Hour: {int(peak_row['hour_period'])}:00 ({int(peak_row['total_trips']):,} trips)")
        c.drawString(300, y_position - 25, f"• Avg Volatility (StdDev): {avg_std:,.2f}")
        c.drawString(300, y_position - 40, f"• Avg Days Observed: {int(avg_obs)}")
        
        # --- B. Generate Plots ---
        plot_total_buf = create_mobility_plot(
            x_data=day_data['hour_period'],
            y_data=day_data['total_trips'],
            title=f"Total Volume: {day_name}",
            y_label="Total Trips",
            color='#007acc' 
        )
        
        plot_avg_buf = create_mobility_plot(
            x_data=day_data['hour_period'],
            y_data=day_data['avg_trips'],
            title=f"Typical Pattern (Average): {day_name}",
            y_label="Avg Trips",
            color='#2ca02c' 
        )
        
        # --- C. Draw Images on PDF ---
        plot_height = 200
        y_pos_plot1 = y_position - 260
        img_total = ImageReader(plot_total_buf)
        c.drawImage(img_total, 40, y_pos_plot1, width=500, height=plot_height)
        
        y_pos_plot2 = y_pos_plot1 - 210
        img_avg = ImageReader(plot_avg_buf)
        c.drawImage(img_avg, 40, y_pos_plot2, width=500, height=plot_height)
        
        y_position -= 500 

    # 4. Finalize & Save PDF
    c.save()
    pdf_buffer.seek(0)

    try:
        print(f"💾 Saving PDF report to: {pdf_path}")
        with open(pdf_path, "wb") as f:
            f.write(pdf_buffer.read())
        print("✅ PDF saved.")
    except Exception as e:
        print(f"❌ Failed to save PDF: {e}")
        raise

    return {"pdf_path": str(pdf_path), "csv_path": str(csv_path)}