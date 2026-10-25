"""
Example task definitions.
These are sample tasks that demonstrate the task queue system.
"""
import time
import random
from worker import task


@task("send_email")
def send_email_task(payload):
    """
    Simulates sending an email.
    In production, this would use SMTP or an email service API.
    """
    recipient = payload.get("to")
    subject = payload.get("subject")
    
    print(f"  → Sending email to {recipient}: {subject}")
    
    # Simulate network delay
    time.sleep(random.uniform(0.5, 2.0))
    
    # Simulate occasional failures (10% chance)
    if random.random() < 0.1:
        raise Exception("SMTP connection failed")
    
    print(f"  ✓ Email sent to {recipient}")
    return {"status": "sent", "recipient": recipient}


@task("process_image")
def process_image_task(payload):
    """
    Simulates image processing (resize, compress, etc.).
    In production, this would use PIL/Pillow or similar.
    """
    image_url = payload.get("url")
    operation = payload.get("operation", "resize")
    
    print(f"  → Processing image: {image_url} (operation: {operation})")
    
    # Simulate CPU-intensive work
    time.sleep(random.uniform(1.0, 3.0))
    
    print(f"  ✓ Image processed: {image_url}")
    return {"status": "processed", "url": image_url}


@task("generate_report")
def generate_report_task(payload):
    """
    Simulates generating a report (PDF, Excel, etc.).
    In production, this would query databases and create files.
    """
    report_type = payload.get("type", "monthly")
    user_id = payload.get("user_id")
    
    print(f"  → Generating {report_type} report for user {user_id}")
    
    # Simulate database queries and file generation
    time.sleep(random.uniform(2.0, 5.0))
    
    # Simulate occasional failures (15% chance)
    if random.random() < 0.15:
        raise Exception("Database connection timeout")
    
    print(f"  ✓ Report generated for user {user_id}")
    return {"status": "generated", "report_type": report_type}


@task("sync_data")
def sync_data_task(payload):
    """
    Simulates syncing data with an external service.
    In production, this would call external APIs.
    """
    source = payload.get("source")
    destination = payload.get("destination")
    
    print(f"  → Syncing data from {source} to {destination}")
    
    # Simulate API calls
    time.sleep(random.uniform(1.0, 4.0))
    
    print(f"  ✓ Data synced: {source} → {destination}")
    return {"status": "synced", "source": source, "destination": destination}


# All task handlers for easy registration
ALL_TASKS = [
    ("send_email", send_email_task),
    ("process_image", process_image_task),
    ("generate_report", generate_report_task),
    ("sync_data", sync_data_task),
]

