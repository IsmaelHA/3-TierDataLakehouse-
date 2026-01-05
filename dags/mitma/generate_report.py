import io
import boto3
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

def generate_mobility_report_s3(con, target_origins: list, bucket_name: str, s3_key: str = "reports/mobility_report.pdf"):
    print(f"📊 Generating Aggregated Report for Origins: {target_origins}")

    # 1. Prepare SQL with Dynamic List of Origins
    placeholders = ', '.join(['?'] * len(target_origins))
    
    query = f"""
        SELECT 
            day_type, 
            hour_period,
            SUM(total_trips) as total_trips,
            AVG(total_trips) as avg_trips,
            STDDEV_SAMP(total_trips) as std_trips,
            AVG(num_days_observed) as num_days_observed
        FROM {GOLD_MITMA_TABLE}
        WHERE origin_zone IN ({placeholders})
        GROUP BY day_type, hour_period
        ORDER BY day_type, hour_period
    """
    
    df = con.execute(query, target_origins).fetch_df()
    
    if df.empty:
        print("⚠️ No data found for these origins. Report skipped.")
        return

    s3_client = boto3.client('s3')

    # --- NEW STEP: Upload CSV Data ---
    try:
        # Create CSV filename based on the PDF key (e.g., reports/report.pdf -> reports/report.csv)
        csv_s3_key = s3_key.replace(".pdf", ".csv")
        
        # Create in-memory buffer for CSV
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)
        
        # Convert string buffer to bytes buffer for upload_fileobj
        csv_bytes_buffer = io.BytesIO(csv_buffer.getvalue().encode('utf-8'))
        
        print(f"💾 Uploading CSV data to s3://{bucket_name}/{csv_s3_key}...")
        s3_client.upload_fileobj(csv_bytes_buffer, bucket_name, csv_s3_key)
        print("✅ CSV Upload successful.")
        
    except Exception as e:
        print(f"❌ Failed to upload CSV: {e}")
        # We don't raise here because we still want to try generating the PDF


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
    c.drawString(50, height - 105, f"Aggregated Report for Origin Zones: {', '.join(map(str, target_origins))}")
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

    # 4. Finalize & Upload PDF
    c.save()
    pdf_buffer.seek(0)

    try:
        print(f"💾 Uploading PDF report to s3://{bucket_name}/{s3_key}...")
        s3_client.upload_fileobj(pdf_buffer, bucket_name, s3_key)
        print("✅ PDF Upload successful.")
    except Exception as e:
        print(f"❌ Failed to upload PDF to S3: {e}")
        raise e