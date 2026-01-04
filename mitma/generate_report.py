import io
import boto3
import matplotlib.pyplot as plt
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib.utils import ImageReader
from ducklake_utils import GOLD_MITMA_TABLE

def get_day_type_name(dt):
    mapping = {
        0: "Sunday", 1: "Monday", 2: "Tue-Thu (Standard)", 
        5: "Friday", 6: "Saturday", 8: "Holiday"
    }
    return mapping.get(dt, f"Unknown ({dt})")

def generate_mobility_report_s3(con, bucket_name: str, s3_key: str = "reports/mobility_report.pdf"):
    print(f" Generating PDF Report for S3 upload: s3://{bucket_name}/{s3_key}")
    
    # 1. Fetch Aggregated Data
    df = con.execute(f"""
        SELECT day_type, hour_period, SUM(avg_trips) as total_hourly_trips
        FROM {GOLD_MITMA_TABLE}
        GROUP BY day_type, hour_period
        ORDER BY day_type, hour_period
    """).fetch_df()
    
    if df.empty:
        print("⚠️ No data found. Report skipped.")
        return

    # 2. Setup In-Memory PDF Buffer
    # We write to this 'pdf_buffer' variable instead of a file on disk
    pdf_buffer = io.BytesIO()
    
    c = canvas.Canvas(pdf_buffer, pagesize=A4)
    width, height = A4
    
    # Title Page
    c.setFont("Helvetica-Bold", 24)
    c.drawString(50, height - 80, "MITMA Mobility Analysis Report")
    c.setFont("Helvetica", 14)
    c.drawString(50, height - 110, "Typical Day Patterns (Gold Layer)")
    
    y_position = height - 160
    unique_days = df['day_type'].unique()
    
    # 3. Iterate and Plot
    for dt in unique_days:
        day_name = get_day_type_name(dt)
        day_data = df[df['day_type'] == dt]
        
        if y_position < 300:
            c.showPage()
            y_position = height - 100
        
        # Create Plot (Matplotlib)
        plt.figure(figsize=(8, 4))
        plt.plot(day_data['hour_period'], day_data['total_hourly_trips'], 
                 marker='o', linestyle='-', color='#007acc', linewidth=2)
        plt.title(f"Hourly Mobility Profile: {day_name}", fontsize=12)
        plt.ylabel("Avg Total Trips")
        plt.grid(True, linestyle='--', alpha=0.6)
        plt.xticks(range(0, 24))
        plt.tight_layout()
        
        # Save Plot to Buffer
        plot_buf = io.BytesIO()
        plt.savefig(plot_buf, format='png', dpi=100)
        plot_buf.seek(0)
        plt.close()
        
        # Draw on PDF
        c.setFont("Helvetica-Bold", 16)
        c.drawString(50, y_position, day_name)
        
        total_vol = day_data['total_hourly_trips'].sum()
        peak_row = day_data.loc[day_data['total_hourly_trips'].idxmax()]
        
        c.setFont("Helvetica", 10)
        c.drawString(50, y_position - 20, f"Total Daily Volume: {int(total_vol):,}")
        c.drawString(50, y_position - 35, f"Peak Hour: {int(peak_row['hour_period'])}:00 ({int(peak_row['total_hourly_trips']):,} trips)")
        
        img = ImageReader(plot_buf)
        c.drawImage(img, 50, y_position - 280, width=500, height=250)
        y_position -= 320

    # 4. Finalize PDF in Buffer
    c.save()
    
    # IMPORTANT: Reset buffer position to the beginning before reading/uploading
    pdf_buffer.seek(0)

    # 5. Upload to S3
    try:
        s3_client = boto3.client('s3')
        s3_client.upload_fileobj(pdf_buffer, bucket_name, s3_key)
        print(f"✅ Report successfully uploaded to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"❌ Failed to upload to S3: {e}")
        raise e