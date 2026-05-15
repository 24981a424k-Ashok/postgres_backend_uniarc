import os
import sys
from datetime import datetime, timedelta
import logging

# Add project root to sys.path
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from src.database.models import SessionLocal, RawNews, VerifiedNews, SavedArticle, TopicTracking, ReadHistory, BreakingNews, TrackNotification

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("DataRetention")

def prune_old_data(days=10):
    """
    Deletes news articles older than the specified number of days (default 10) to save space and egress.
    Articles saved by users or tracked are preserved.
    """
    db = SessionLocal()
    threshold = datetime.utcnow() - timedelta(days=days)
    
    try:
        logger.info(f"Starting data pruning (Threshold: articles older than {threshold})")
        
        # 1. Get IDs of articles that should NOT be deleted
        logger.info("Identifying protected articles...")
        protected_ids = set()
        
        # Saved by users
        saved_ids = db.query(SavedArticle.news_id).all()
        protected_ids.update([sid[0] for sid in saved_ids if sid[0]])
        
        # Tracked by users
        tracked_ids = db.query(TopicTracking.news_id).all()
        protected_ids.update([tid[0] for tid in tracked_ids if tid[0]])
        
        # Notifications sent
        notify_ids = db.query(TrackNotification.news_id).all()
        protected_ids.update([nid[0] for nid in notify_ids if nid[0]])
        
        # Breaking news references
        breaking_ids = db.query(BreakingNews.verified_news_id).all()
        protected_ids.update([bid[0] for bid in breaking_ids if bid[0]])
        
        logger.info(f"Protected VerifiedNews count: {len(protected_ids)}")
        
        # 2. Delete old VerifiedNews
        # Filter: older than threshold AND not in protected list
        verified_query = db.query(VerifiedNews).filter(
            VerifiedNews.published_at < threshold,
            ~VerifiedNews.id.in_(list(protected_ids)) if protected_ids else True
        )
        verified_to_delete = verified_query.count()
        if verified_to_delete > 0:
            logger.info(f"Deleting {verified_to_delete} old VerifiedNews records...")
            verified_query.delete(synchronize_session=False)
        else:
            logger.info("No old VerifiedNews to delete.")
            
        # 3. Delete old RawNews
        # Filter: older than threshold AND NOT referenced by any VerifiedNews
        logger.info("Identifying RawNews referenced by any VerifiedNews...")
        referenced_raw_ids = db.query(VerifiedNews.raw_news_id).filter(VerifiedNews.raw_news_id != None).all()
        protected_raw_ids = set([r[0] for r in referenced_raw_ids])
        
        raw_query = db.query(RawNews).filter(
            RawNews.published_at < threshold,
            ~RawNews.id.in_(list(protected_raw_ids)) if protected_raw_ids else True
        )
        raw_to_delete = raw_query.count()
        if raw_to_delete > 0:
            logger.info(f"Deleting {raw_to_delete} old RawNews records...")
            raw_query.delete(synchronize_session=False)
        else:
            logger.info("No old RawNews to delete.")
            
        # 4. Cleanup old ReadHistory (optional but good for DB size)
        # Keep 30 days of history?
        history_threshold = datetime.utcnow() - timedelta(days=30)
        history_query = db.query(ReadHistory).filter(ReadHistory.read_at < history_threshold)
        history_to_delete = history_query.count()
        if history_to_delete > 0:
            logger.info(f"Deleting {history_to_delete} old ReadHistory records...")
            history_query.delete(synchronize_session=False)

        db.commit()
        logger.info("Pruning completed successfully.")
        
    except Exception as e:
        db.rollback()
        logger.error(f"Error during pruning: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    # Default to 10 days as requested
    prune_old_data(days=10)
