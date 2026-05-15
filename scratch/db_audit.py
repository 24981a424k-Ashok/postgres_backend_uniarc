
import os
import sys
from datetime import datetime
from sqlalchemy import func
from src.database.models import SessionLocal, VerifiedNews, RawNews

def audit_database():
    db = SessionLocal()
    try:
        # 1. Date Range
        first_article = db.query(VerifiedNews).order_by(VerifiedNews.published_at.asc()).first()
        last_article = db.query(VerifiedNews).order_by(VerifiedNews.published_at.desc()).first()
        
        print(f"--- ARTICLE AGE AUDIT ---")
        if first_article:
            print(f"Earliest article date: {first_article.published_at}")
        else:
            print("No articles found in verified_news.")
            
        if last_article:
            print(f"Latest article date: {last_article.published_at}")
            
        # 2. Volume Audit
        verified_count = db.query(VerifiedNews).count()
        raw_count = db.query(RawNews).count()
        print(f"\n--- VOLUME AUDIT ---")
        print(f"Total verified articles: {verified_count}")
        print(f"Total raw articles: {raw_count}")
        
        # 3. Payload Size Estimation (Heuristic)
        print(f"\n--- PAYLOAD SIZE ESTIMATION (Average chars) ---")
        avg_content = db.query(func.avg(func.length(VerifiedNews.content))).scalar() or 0
        avg_why = db.query(func.avg(func.length(VerifiedNews.why_it_matters))).scalar() or 0
        avg_who = db.query(func.avg(func.length(VerifiedNews.who_is_affected))).scalar() or 0
        
        print(f"Average 'content' length: {int(avg_content)} chars")
        print(f"Average 'why_it_matters' length: {int(avg_why)} chars")
        print(f"Average 'who_is_affected' length: {int(avg_who)} chars")
        
        # 4. Oldest articles identification
        one_month_ago = datetime.utcnow().replace(month=datetime.utcnow().month-1) if datetime.utcnow().month > 1 else datetime.utcnow().replace(year=datetime.utcnow().year-1, month=12)
        old_articles_count = db.query(VerifiedNews).filter(VerifiedNews.published_at < one_month_ago).count()
        print(f"\nArticles older than 1 month: {old_articles_count}")

    except Exception as e:
        print(f"Error during audit: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    audit_database()
